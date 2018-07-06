package app

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	ma "github.com/akshaykarle/go-mongodbatlas/mongodbatlas"
	"github.com/honeycombio/honeytail/event"
	"github.com/honeycombio/honeytail/parsers/mongodb"
	libhoney "github.com/honeycombio/libhoney-go"
	dac "github.com/xinsnake/go-http-digest-auth-client"
	"golang.org/x/sync/semaphore"
)

const (
	refreshInterval = time.Second * 60
	maxConcurrency  = 10
)

type Options struct {
	Username        string        `long:"username" description:"Your Atlas Username" required:"true"`
	APIKey          string        `long:"api_key" description:"Your Atlas API Key" required:"true"`
	GroupID         string        `long:"group_id" description:"Your Atlas Group ID" required:"true"`
	Clusters        []string      `long:"cluster" description:"Cluster to gather logs from. Maybe specified multiple times." required:"true"`
	APIHost         string        `long:"api_host" description:"Hostname for the Honeycomb API server" default:"https://api.honeycomb.io/"`
	WriteKey        string        `long:"writekey" description:"Your Honeycomb write key." required:"true"`
	Dataset         string        `long:"dataset" description:"Target Honeycomb dataset" default:"mongodb-atlas-logs"`
	NumParsers      int           `long:"num_parsers" description:"Number of parsers to use in parallel to process log lines" default:"4"`
	PollingInterval time.Duration `long:"polling_interval" description:"Time between each request to the Atlas API for more logs." default:"5m"`
}

type app struct {
	options *Options
	state   *atlasState
	mux     sync.RWMutex
}

// atlasState represents what we know about the Atlas cluster(s) we are
// collecting from as of the last API call. We need to refresh this
// periodically to avoid collecting from stale nodes, and pick up new nodes
type atlasState struct {
	clusters   []ma.Cluster
	collectors map[string]*collector
	parser     *mongodb.Parser
	lines      chan string
	event      chan event.Event
}

func NewApp(options *Options) *app {
	return &app{
		options: options,
		state:   &atlasState{},
		mux:     sync.RWMutex{},
	}
}

func (a *app) fetchAtlasState() error {
	t := dac.NewTransport(a.options.Username, a.options.APIKey)
	httpClient := &http.Client{Transport: &t}
	client := ma.NewClient(httpClient)
	allClusters, _, err := client.Clusters.List(a.options.GroupID)
	if err != nil {
		return err
	}

	clusters := []ma.Cluster{}
	for _, cluster := range allClusters {
		for _, clusterName := range a.options.Clusters {
			if cluster.Name == clusterName {
				clusters = append(clusters, cluster)
			}
		}
	}
	if len(clusters) != len(a.options.Clusters) {
		err := fmt.Errorf("unable to find all expected cluster names in atlas")
		logrus.WithFields(logrus.Fields{
			"expected_count": len(a.options.Clusters),
			"actual_count":   len(clusters),
			"expected":       a.options.Clusters,
		}).WithError(err).Error()
		return err
	}

	a.mux.Lock()
	defer a.mux.Unlock()
	a.state.clusters = clusters
	return nil
}

func (a *app) Run() {
	a.state.lines = make(chan string)
	a.state.event = make(chan event.Event)
	a.state.parser = &mongodb.Parser{}
	a.state.parser.Init(&mongodb.Options{
		NumParsers:  a.options.NumParsers,
		LogPartials: true,
	})
	go a.state.parser.ProcessLines(a.state.lines, a.state.event, nil)
	go sendEvents(a.state.event)
	for ; ; time.Sleep(refreshInterval) {
		if err := a.fetchAtlasState(); err != nil {
			logrus.WithError(err).Error("failed to get cluser state from atlas, will retry")
			continue
		}
		a.ensureCollectors()
	}
}

func (a *app) ensureCollectors() {
	if a.state == nil {
		return
	}

	if a.state.collectors == nil {
		a.state.collectors = make(map[string]*collector)
	}

	allNodes := map[string]struct{}{}
	for _, cluster := range a.state.clusters {
		nodes := parseNodes(cluster.MongoURI)
		for _, node := range nodes {
			// mark this node as present in the current state
			allNodes[node] = struct{}{}

			// we have a collector for this node already
			if _, ok := a.state.collectors[node]; ok {
				continue
			}

			collector := newCollector(a.options.GroupID, cluster.Name, node,
				"mongodb.gz", a.options.Username, a.options.APIKey, a.state.lines,
				a.options.PollingInterval)
			a.state.collectors[node] = collector
			collector.Start()
		}
	}

	// now time to prune collectors that don't exist anymore
	for node := range a.state.collectors {
		if _, ok := allNodes[node]; !ok {
			a.state.collectors[node].Stop()
			delete(a.state.collectors, node)
		}
	}
}

type logRequest struct {
	startDate int64
	endDate   int64
}

type collector struct {
	groupID         string
	clusterName     string
	hostName        string
	logName         string
	username        string
	apikey          string
	lines           chan string
	errorCount      int
	stop            bool
	pendingRequests chan *logRequest
	logs            chan string
	reqSem          *semaphore.Weighted
	logSem          *semaphore.Weighted
	interval        time.Duration
}

func newCollector(groupID, clusterName, hostName, logName, username, apikey string, lines chan string, interval time.Duration) *collector {
	return &collector{
		groupID:         groupID,
		clusterName:     clusterName,
		hostName:        hostName,
		logName:         logName,
		username:        username,
		apikey:          apikey,
		lines:           lines,
		pendingRequests: make(chan *logRequest),
		logs:            make(chan string),
		reqSem:          semaphore.NewWeighted(maxConcurrency),
		logSem:          semaphore.NewWeighted(maxConcurrency),
		interval:        interval,
	}
}

func (c *collector) Stop() {
	c.stop = true
}

func (c *collector) Start() {
	go c.execute()
}

func (c *collector) execute() {
	go c.pullLog()
	go c.readLog()

	for ; ; time.Sleep(c.interval) {
		if c.stop {
			close(c.logs)
			close(c.pendingRequests)
			return
		}

		// target read time is approximately every minute
		now := time.Now()
		// enqueue the request to be fulfilled by `pullLog`
		c.pendingRequests <- &logRequest{
			endDate:   now.Unix(),
			startDate: now.Add(c.interval * time.Duration(-1)).Unix(),
		}
	}
}

func (c *collector) pullLog() {
	ctx := context.TODO()
	prefix := fmt.Sprintf("%s-%s", c.clusterName, c.hostName)
	for {
		select {
		case r := <-c.pendingRequests:
			if err := c.reqSem.Acquire(ctx, 1); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"cluster":  c.clusterName,
					"hostname": c.hostName,
					"logName":  c.logName,
				}).Error("pullLog got unexpected error while acquiring semaphore")
				continue
			}
			// after acquiring a semaphore, dispatch a routine to pull the log
			// and stage it as a local file. This model allows us to pull
			// multiple files, in case we're behind, while not creating too many
			// go routines
			go func() {
				defer c.reqSem.Release(1)
				t := dac.NewTransport(c.username, c.apikey)
				httpClient := &http.Client{Transport: &t}

				url := fmt.Sprintf(
					"https://cloud.mongodb.com/api/atlas/v1.0/groups/%s/clusters/%s/logs/%s?startDate=%d&endDate=%d",
					c.groupID, c.hostName, c.logName, r.startDate, r.endDate)
				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":  c.clusterName,
						"hostname": c.hostName,
						"logName":  c.logName,
					}).Error("failed to build API request for logs")
					return
				}

				req.Header.Add("Accept-Encoding", "gzip")
				resp, err := httpClient.Do(req)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":   c.clusterName,
						"hostname":  c.hostName,
						"logName":   c.logName,
						"startDate": r.startDate,
						"endDate":   r.endDate,
					}).Error("failed to request log for host")
					return
				}
				if resp.StatusCode != 200 {
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":   c.clusterName,
						"hostname":  c.hostName,
						"logName":   c.logName,
						"startDate": r.startDate,
						"endDate":   r.endDate,
						"status":    resp.StatusCode,
					}).Error("bad status code for request")
					return
				}
				defer resp.Body.Close()

				outFile, err := ioutil.TempFile(os.TempDir(), prefix)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":   c.clusterName,
						"hostname":  c.hostName,
						"logName":   c.logName,
						"startDate": r.startDate,
						"endDate":   r.endDate,
					}).Error("unable to open log file for writing")
					return
				}
				defer outFile.Close()

				written, err := io.Copy(outFile, resp.Body)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":   c.clusterName,
						"hostname":  c.hostName,
						"logName":   c.logName,
						"startDate": r.startDate,
						"endDate":   r.endDate,
						"file":      outFile.Name(),
					}).Error("unable to write log file")
					syscall.Unlink(outFile.Name())
					return
				}

				logrus.WithFields(logrus.Fields{
					"cluster":      c.clusterName,
					"hostname":     c.hostName,
					"logName":      c.logName,
					"startDate":    r.startDate,
					"endDate":      r.endDate,
					"file":         outFile.Name(),
					"bytesWritten": written,
				}).Info("staged log file for ingestion")

				c.logs <- outFile.Name()
			}()
		}
	}
}

func (c *collector) readLog() {
	ctx := context.TODO()

	for {
		select {
		case fileName := <-c.logs:
			if err := c.logSem.Acquire(ctx, 1); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"cluster":  c.clusterName,
					"hostname": c.hostName,
					"logName":  c.logName,
				}).Error("readLog got unexpected error while acquiring semaphore")
				continue
			}
			// after acquiring a semaphore, dispatch a routine to read the log
			// and feed it line-by-line to the parser. This model allows us to
			// parallelize some I/O in case we get behind, while not creating
			// too many go routines. Ultimately we will block on the parser's
			// throughput
			go func() {
				defer c.logSem.Release(1)
				defer syscall.Unlink(fileName)
				file, err := os.Open(fileName)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":  c.clusterName,
						"hostname": c.hostName,
						"logName":  c.logName,
						"fileName": fileName,
					}).Error("unable to open log file for writing")
					syscall.Unlink(fileName)
					return
				}

				// file contents are gzipped
				gzipReader, err := gzip.NewReader(file)
				if err != nil {
					if err == io.EOF {
						logrus.WithError(err).WithFields(logrus.Fields{
							"cluster":  c.clusterName,
							"hostname": c.hostName,
							"logName":  c.logName,
							"fileName": fileName,
						}).Info("got EOF, possibly no log data for this time window")
						return
					}
					logrus.WithError(err).WithFields(logrus.Fields{
						"cluster":  c.clusterName,
						"hostname": c.hostName,
						"logName":  c.logName,
						"fileName": fileName,
					}).Error("failed to build gzip reader")
					return
				}
				defer gzipReader.Close()

				reader := bufio.NewReader(gzipReader)
				for {

					line, err := reader.ReadString('\n')
					if err == io.EOF {
						break
					} else if err != nil {
						logrus.WithError(err).Warn("failed to read line")
						continue
					}

					c.lines <- line
				}
			}()
		}
	}
}

func parseNodes(mongoURI string) []string {
	s1 := strings.TrimPrefix(mongoURI, "mongodb://")
	s2 := strings.Split(s1, ",")
	nodes := []string{}
	for _, node := range s2 {
		s3 := strings.Split(node, ":")
		nodes = append(nodes, s3[0])
	}

	return nodes
}

func sendEvents(events <-chan event.Event) {
	for {
		select {
		case ev := <-events:
			logrus.WithFields(ev.Data).Debug("Got Event")
			e := libhoney.NewEvent()
			e.Add(ev.Data)
			e.Timestamp = ev.Timestamp
			e.Send()
		}
	}
}

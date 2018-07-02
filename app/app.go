package app

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	ma "github.com/akshaykarle/go-mongodbatlas/mongodbatlas"
	"github.com/honeycombio/honeytail/event"
	"github.com/honeycombio/honeytail/parsers/mongodb"
	libhoney "github.com/honeycombio/libhoney-go"
	dac "github.com/xinsnake/go-http-digest-auth-client"
)

const (
	refreshInterval = time.Second * 60
	numParsers      = 4
)

type Options struct {
	Username string   `long:"username" description:"Your Atlas Username" required:"true"`
	APIKey   string   `long:"api_key" description:"Your Atlas API Key" required:"true"`
	GroupID  string   `long:"group_id" description:"Your Atlas Group ID" required:"true"`
	Clusters []string `long:"cluster" description:"Cluster to gather logs from. Maybe specified multiple times." required:"true"`
	APIHost  string   `long:"api_host" description:"Hostname for the Honeycomb API server" default:"https://api.honeycomb.io/"`
	WriteKey string   `long:"writekey" description:"Your Honeycomb write key." required:"true"`
	Dataset  string   `long:"dataset" description:"Target Honeycomb dataset" default:"mongodb-atlas-logs"`
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
		NumParsers:  numParsers,
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
				"mongodb.gz", a.options.Username, a.options.APIKey, a.state.lines)
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

type collector struct {
	groupID     string
	clusterName string
	hostName    string
	logName     string
	username    string
	apikey      string
	lines       chan string
	lastRead    time.Time
	errorCount  int
	stop        bool
}

func newCollector(groupID, clusterName, hostName, logName, username, apikey string, lines chan string) *collector {
	return &collector{
		groupID:     groupID,
		clusterName: clusterName,
		hostName:    hostName,
		logName:     logName,
		username:    username,
		apikey:      apikey,
		lines:       lines,
	}
}

func (c *collector) Stop() {
	c.stop = true
}

func (c *collector) Start() {
	go c.execute()
}

func (c *collector) execute() {
	sleepDuration := refreshInterval

	for ; ; time.Sleep(sleepDuration) {
		if c.stop {
			return
		}
		timeStart := time.Now()
		c.pullLog()

		// target sleep interval is refreshInterval, but we update it to
		// subtract the time we spent doing work
		sleepDuration = refreshInterval - time.Since(timeStart)
	}
}

func (c *collector) pullLog() {
	var startDate, endDate int64
	// if this is the first time we've read, set the read window to the last 1 minute
	if c.lastRead.IsZero() {
		c.lastRead = time.Now()
		endDate = c.lastRead.Unix()
		startDate = c.lastRead.Add(time.Duration(-1) * time.Minute).Unix()
	} else {
		// otherwise, read from the last read time until now
		startDate = c.lastRead.Unix()
		c.lastRead = time.Now()
		endDate = c.lastRead.Unix()
	}

	t := dac.NewTransport(c.username, c.apikey)
	httpClient := &http.Client{Transport: &t}

	url := fmt.Sprintf(
		"https://cloud.mongodb.com/api/atlas/v1.0/groups/%s/clusters/%s/logs/%s?startDate=%d&endDate=%d",
		c.groupID, c.hostName, c.logName, startDate, endDate)
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
			"startDate": startDate,
			"endDate":   endDate,
		}).Error("failed to read log for host")
		return
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		if err == io.EOF {
			logrus.WithError(err).WithFields(logrus.Fields{
				"cluster":   c.clusterName,
				"hostname":  c.hostName,
				"logName":   c.logName,
				"startDate": startDate,
				"endDate":   endDate,
			}).Info("got EOF, possibly no log data for this time window")
			return
		}
		logrus.WithError(err).WithFields(logrus.Fields{
			"cluster":   c.clusterName,
			"hostname":  c.hostName,
			"logName":   c.logName,
			"startDate": startDate,
			"endDate":   endDate,
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

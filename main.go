package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/honeycombio/atlas/app"
	"github.com/honeycombio/honeytail/parsers/mongodb"
	libhoney "github.com/honeycombio/libhoney-go"

	flag "github.com/jessevdk/go-flags"
)

func parseFlags() (*app.Options, error) {
	var options app.Options
	parser := flag.NewParser(&options, flag.Default)
	if extraArgs, err := parser.Parse(); err != nil || len(extraArgs) != 0 {
		if err != nil {
			if err.(*flag.Error).Type == flag.ErrHelp {
				// user specified --help
				os.Exit(0)
			}
			return nil, fmt.Errorf("Failed to parse the command line. Run with --help for more info")
		}
		return nil, fmt.Errorf("unexpected extra arguments: %s", strings.Join(extraArgs, " "))
	}

	return &options, nil
}

func main() {
	p := mongodb.MongoLineParser{}
	p.ParseLine("a")

	options, err := parseFlags()
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	libhoney.Init(libhoney.Config{
		WriteKey: options.WriteKey,
		APIHost:  options.APIHost,
		Dataset:  options.Dataset,
	})

	app := app.NewApp(options)
	app.Run()
}

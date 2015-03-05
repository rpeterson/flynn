package main

import (
	"io"
	"os"
	"strconv"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-docopt"
	"github.com/flynn/flynn/controller/client"
)

func init() {
	register("log", runLog, `
usage: flynn log [options]

Stream log for an app.

Options:
	-f, --follow         stream new lines after printing log buffer
	-n, --number <lines> return at most n lines from the log buffer
	-j, --job <id>       filter logs to a specific job ID
	-s, --split-stderr   send stderr lines to stderr
`)
}

func runLog(args *docopt.Args, client *controller.Client) error {
	lines := 0
	if strlines := args.String["--number"]; strlines != "" {
		var err error
		if lines, err = strconv.Atoi(strlines); err != nil {
			return err
		}
	}
	rc, err := client.GetAppLog(mustApp(), lines, args.Bool["--follow"])
	if err != nil {
		return err
	}
	defer rc.Close()

	// TODO: don't want to copy raw bytes to stdout, want to deserialize the
	// logaggregator client.Message structs and print the message portion with
	// some Heroku-like display logic.
	if _, err = io.Copy(os.Stdout, rc); err != nil {
		return err
	}
	return nil
}

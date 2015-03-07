package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/appliance/postgresql/state"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/httphelper"
	"github.com/flynn/flynn/pkg/shutdown"
)

func main() {
	serviceName := os.Getenv("FLYNN_POSTGRES")
	if serviceName == "" {
		serviceName = "postgres"
	}
	singleton := os.Getenv("SINGLETON") == "true"
	password := os.Getenv("PGPASSWORD")

	err := discoverd.DefaultClient.AddService(serviceName, &discoverd.ServiceConfig{
		LeaderType: discoverd.LeaderTypeManual,
	})
	if je, ok := err.(httphelper.JSONError); err != nil && (!ok || je.Code != httphelper.ObjectExistsError) {
		shutdown.Fatal(err)
	}
	inst := &discoverd.Instance{Addr: ":5432"}
	hb, err := discoverd.DefaultClient.RegisterInstance(serviceName, inst)
	if err != nil {
		shutdown.Fatal(err)
	}
	shutdown.BeforeExit(func() { hb.Close() })

	log := log15.New("app", "postgres")

	pg := NewPostgres(Config{
		ID:           inst.ID,
		Singleton:    singleton,
		BinDir:       "/usr/lib/postgresql/9.4/bin/",
		Password:     password,
		Logger:       log.New("component", "postgres"),
		ExtWhitelist: true,
		// TODO(titanous) investigate this:
		SHMType: "sysv", // the default on 9.4, 'posix' is not currently supported in our containers
	})
	dd := NewDiscoverd(discoverd.DefaultClient.Service(serviceName), log.New("component", "discoverd"))

	peer := state.NewPeer(inst, singleton, dd, pg, log.New("component", "peer"))
	shutdown.BeforeExit(func() { peer.Close() })

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Signal(syscall.SIGUSR1))
		<-ch
		log.Info("received USR1 signal, unregistering and stopping postgres")
		hb.Close()
		peer.Stop()
	}()

	peer.Run()
	// TODO(titanous): clean shutdown of postgres

	// peer.Run will return in two cases:
	//
	// 1) SIGTERM / SIGINT received - shutdown package will call os.Exit
	// 2) SIGUSR1 received - a deployment is indicating postgres needs to
	//    be stopped and the peer removed from the cluster, but the process
	//    should stay up so the scheduler does not replace with a new job
	//    (the deployer starts postgres jobs manually).
	//
	// In both cases, we should block the main function.
	select {}
}

package main

import (
	"encoding/json"
	"fmt"
	"time"

	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/appliance/postgresql/state"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/postgres"
)

type PostgresSuite struct {
	Helper
}

var _ = c.ConcurrentSuite(&PostgresSuite{})

// Check postgres config to avoid regressing on https://github.com/flynn/flynn/issues/101
func (s *PostgresSuite) TestSSLRenegotiationLimit(t *c.C) {
	query := flynn(t, "/", "-a", "controller", "psql", "--", "-c", "SHOW ssl_renegotiation_limit")
	t.Assert(query, Succeeds)
	t.Assert(query, OutputContains, "ssl_renegotiation_limit \n-------------------------\n 0\n(1 row)")
}

func (s *PostgresSuite) TestDeployNormalMode(t *c.C) {
	// create postgres app
	client := s.controllerClient(t)
	name := "postgres-deploy"
	app := &ct.App{Name: name, Strategy: "postgres"}
	t.Assert(client.CreateApp(app), c.IsNil)

	// copy release from default postgres app
	release, err := client.GetAppRelease("postgres")
	t.Assert(err, c.IsNil)
	release.ID = ""
	proc := release.Processes["postgres"]
	delete(proc.Env, "SINGLETON")
	proc.Env["FLYNN_POSTGRES"] = name
	proc.Service = name
	release.Processes["postgres"] = proc
	t.Assert(client.CreateRelease(release), c.IsNil)
	t.Assert(client.SetAppRelease(app.ID, release.ID), c.IsNil)
	oldRelease := release.ID

	// start 5 postgres processes and wait for the corresponding cluster state
	discEvents := make(chan *discoverd.Event)
	discStream, err := s.discoverdClient(t).Service(name).Watch(discEvents)
	t.Assert(err, c.IsNil)
	defer discStream.Close()
	t.Assert(client.PutFormation(&ct.Formation{
		AppID:     app.ID,
		ReleaseID: release.ID,
		Processes: map[string]int{"postgres": 5},
	}), c.IsNil)
	getState := func() state.State {
		for {
			select {
			case event := <-discEvents:
				if event.Kind != discoverd.EventKindServiceMeta {
					continue
				}
				var state state.State
				t.Assert(json.Unmarshal(event.ServiceMeta.Data, &state), c.IsNil)
				debugf(t, "got pg cluster state: primary=%t sync=%t async=%d", state.Primary != nil, state.Sync != nil, len(state.Async))
				return state
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for pg cluster state")
			}
		}
	}
	for {
		state := getState()
		if state.Primary != nil && state.Sync != nil && len(state.Async) == 3 {
			break
		}
	}

	// continuously write to the db
	db := postgres.Wait(name, fmt.Sprintf("dbname=postgres user=flynn password=%s", release.Env["PGPASSWORD"]))
	dbname := "deploy-test"
	t.Assert(db.Exec(fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER = "flynn"`, dbname)), c.IsNil)
	db, err = postgres.Open(name, fmt.Sprintf("dbname=%s user=flynn password=%s", dbname, release.Env["PGPASSWORD"]))
	t.Assert(err, c.IsNil)
	t.Assert(db.Exec(`CREATE TABLE deploy_test ( data text)`), c.IsNil)
	dbErrs := make(chan error)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				dbErrs <- db.Exec(`INSERT INTO deploy_test (data) VALUES ('data')`)
			}
		}
	}()

	// check a deploy completes without any db write errors
	release.ID = ""
	t.Assert(client.CreateRelease(release), c.IsNil)
	newRelease := release.ID
	deployment, err := client.CreateDeployment(app.ID, newRelease)
	t.Assert(err, c.IsNil)
	deployEvents := make(chan *ct.DeploymentEvent)
	deployStream, err := client.StreamDeployment(deployment.ID, deployEvents)
	t.Assert(err, c.IsNil)
	defer deployStream.Close()

	type expectedState struct {
		Primary, Sync string
		Async         []string
	}
	expected := []expectedState{
		// kill Async[0], new Async[2]
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, oldRelease}},
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, oldRelease, newRelease}},

		// kill Async[0], new Async[2]
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, newRelease}},
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, newRelease, newRelease}},

		// kill Async[0], new Async[2]
		{Primary: oldRelease, Sync: oldRelease, Async: []string{newRelease, newRelease}},
		{Primary: oldRelease, Sync: oldRelease, Async: []string{newRelease, newRelease, newRelease}},

		// kill Sync, new Async[2]
		{Primary: oldRelease, Sync: newRelease, Async: []string{newRelease, newRelease}},
		{Primary: oldRelease, Sync: newRelease, Async: []string{newRelease, newRelease, newRelease}},

		// kill Primary, new Async[2]
		{Primary: newRelease, Sync: newRelease, Async: []string{newRelease, newRelease}},
		{Primary: newRelease, Sync: newRelease, Async: []string{newRelease, newRelease, newRelease}},
	}

	assertState := func(state state.State, expected expectedState) {
		if state.Primary == nil {
			t.Fatal("no primary configured")
		}
		if state.Sync == nil {
			t.Fatal("no sync configured")
		}
		if len(state.Async) != len(expected.Async) {
			t.Fatalf("expected %d asyncs, got %d", len(expected.Async), len(state.Async))
		}
		if state.Primary.Meta["FLYNN_RELEASE_ID"] != expected.Primary {
			t.Fatal("primary has incorrect release")
		}
		if state.Sync.Meta["FLYNN_RELEASE_ID"] != expected.Sync {
			t.Fatal("sync has incorrect release")
		}
		for i, release := range expected.Async {
			if state.Async[i].Meta["FLYNN_RELEASE_ID"] != release {
				t.Fatalf("async[%d] has incorrect release", i)
			}
		}
	}
	checked := 0
loop:
	for {
		select {
		case e := <-deployEvents:
			switch e.Status {
			case "complete":
				break loop
			case "failed":
				t.Fatalf("deployment failed: %s", e.Error)
			}
			debugf(t, "got deployment event: %s %s", e.JobType, e.JobState)
			if e.JobState == "up" || e.JobState == "down" {
				assertState(getState(), expected[checked])
				checked++
			}
		case err := <-dbErrs:
			t.Assert(err, c.IsNil)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for deployment")
		}
	}
}

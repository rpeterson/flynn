package strategy

import (
	"encoding/json"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/flynn/flynn/appliance/postgresql/state"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/host/types"
	"github.com/flynn/flynn/pkg/cluster"
)

func postgres(d *Deploy) error {
	log := d.logger.New("fn", "postgres")
	log.Info("starting postgres deployment")

	if d.serviceMeta == nil {
		e := "missing pg cluster state"
		log.Error(e)
		return errors.New(e)
	}

	var state state.State
	log.Info("decoding pg cluster state")
	if err := json.Unmarshal(d.serviceMeta.Data, &state); err != nil {
		log.Error("error decoding pg cluster state", "err", err)
		return err
	}

	if state.Primary == nil {
		e := "pg cluster state has no primary, cannot deploy"
		log.Error(e)
		return errors.New(e)
	}

	replaceInstance := func(inst *discoverd.Instance) error {
		d.deployEvents <- ct.DeploymentEvent{
			ReleaseID: d.OldReleaseID,
			JobState:  "stopping",
			JobType:   "postgres",
		}
		id, ok := inst.Meta["FLYNN_JOB_ID"]
		if !ok {
			e := "instance missing FLYNN_JOB_ID metadata"
			log.Error(e)
			return errors.New(e)
		}
		hostID, jobID, err := cluster.ParseJobID(id)
		if err != nil {
			e := fmt.Sprintf("error parsing job id: %q", id)
			log.Error(e)
			return errors.New(e)
		}
		log := log.New("host_id", hostID, "job_id", jobID)
		log.Info("sending SIGUSR1 to job")
		clusterClient, err := cluster.NewClient()
		if err != nil {
			log.Error("error connecting to cluster", "err", err)
			return err
		}
		hostClient, err := clusterClient.DialHost(hostID)
		if err != nil {
			log.Error("error dialling host", "err", err)
			return err
		}
		attachClient, err := hostClient.Attach(&host.AttachReq{JobID: jobID}, false)
		if err != nil {
			log.Error("error attaching to job", "err", err)
			return err
		}
		defer attachClient.Close()
		if err := attachClient.Signal(int(syscall.SIGUSR1)); err != nil {
			log.Error("error sending SIGUSR1 to job", "err", err)
			return err
		}
		log.Info("waiting for instance to go down")
		for {
			select {
			case event := <-d.serviceEvents:
				if event.Kind == discoverd.EventKindDown && event.Instance.ID == inst.ID {
					d.deployEvents <- ct.DeploymentEvent{
						ReleaseID: d.OldReleaseID,
						JobState:  "down",
						JobType:   "postgres",
					}
					return nil
				}
			case <-time.After(30 * time.Second):
				e := "timed out waiting for instance to go down"
				log.Error(e)
				return errors.New(e)
			}
		}
		log.Info("starting new instance")
		d.deployEvents <- ct.DeploymentEvent{
			ReleaseID: d.NewReleaseID,
			JobState:  "starting",
			JobType:   "postgres",
		}
		job, err := d.client.RunJobDetached(d.AppID, &ct.NewJob{
			ReleaseID:  d.NewReleaseID,
			ReleaseEnv: true,
			Cmd:        []string{"postgres"},
			Meta:       map[string]string{"flynn-controller.type": "postgres"},
		})
		if err != nil {
			log.Error("error starting new instance", "err", err)
			return err
		}
		log.Info("waiting for new instance to come up")
		for {
			select {
			case event := <-d.serviceEvents:
				if event.Kind == discoverd.EventKindUp &&
					event.Instance.Meta != nil &&
					event.Instance.Meta["FLYNN_JOB_ID"] == job.ID {
					d.deployEvents <- ct.DeploymentEvent{
						ReleaseID: d.NewReleaseID,
						JobState:  "up",
						JobType:   "postgres",
					}
					return nil
				}
			case <-time.After(30 * time.Second):
				e := "timed out waiting for new instance to come up"
				log.Error(e)
				return errors.New(e)
			}
		}
	}

	if len(state.Async) > 0 {
		for i := 0; i < len(state.Async); i++ {
			log.Info("replacing an Async node")
			if err := replaceInstance(state.Async[0]); err != nil {
				return err
			}
			// TODO: wait for new Async[0] to catch Sync
		}
	}

	if state.Sync != nil {
		log.Info("replacing the Sync node")
		if err := replaceInstance(state.Sync); err != nil {
			return err
		}
		// TODO: wait for new Sync to catch Primary
	}

	log.Info("replacing the Primary node")
	if err := replaceInstance(state.Primary); err != nil {
		return err
	}
	// TODO: wait for valid cluster state
	// TODO: deploy api process type
	return nil
}

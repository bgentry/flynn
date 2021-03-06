package strategy

import (
	"fmt"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
)

type UnknownStrategyError struct {
	Strategy string
}

func (e UnknownStrategyError) Error() string {
	return fmt.Sprintf("deployer: unknown strategy %q", e.Strategy)
}

type Deploy struct {
	*ct.Deployment
	client        *controller.Client
	deployEvents  chan<- ct.DeploymentEvent
	jobEvents     chan *ct.JobEvent
	serviceEvents chan *discoverd.Event
	useJobEvents  map[string]struct{}
	logger        log15.Logger
}

type PerformFunc func(d *Deploy) error

var performFuncs = map[string]PerformFunc{
	"all-at-once": allAtOnce,
	"one-by-one":  oneByOne,
}

func Perform(d *ct.Deployment, client *controller.Client, deployEvents chan<- ct.DeploymentEvent, logger log15.Logger) error {
	log := logger.New("fn", "Perform", "deployment_id", d.ID, "app_id", d.AppID)

	log.Info("validating deployment strategy")
	performFunc, ok := performFuncs[d.Strategy]
	if !ok {
		err := UnknownStrategyError{d.Strategy}
		log.Error("error validating deployment strategy", "err", err)
		return err
	}

	deploy := &Deploy{
		Deployment:    d,
		client:        client,
		deployEvents:  deployEvents,
		serviceEvents: make(chan *discoverd.Event),
		useJobEvents:  make(map[string]struct{}),
		logger:        logger.New("deployment_id", d.ID, "app_id", d.AppID),
	}

	log.Info("determining release services")
	release, err := client.GetRelease(d.NewReleaseID)
	if err != nil {
		log.Error("error getting new release", "release_id", d.NewReleaseID, "err", err)
		return err
	}
	for typ, proc := range release.Processes {
		if proc.Service == "" {
			log.Info(fmt.Sprintf("using job events for %s process type, no service defined", typ))
			deploy.useJobEvents[typ] = struct{}{}
			continue
		}

		log.Info(fmt.Sprintf("using service discovery for %s process type", typ), "service", proc.Service)
		events := make(chan *discoverd.Event)
		stream, err := discoverd.NewService(proc.Service).Watch(events)
		if err != nil {
			log.Error("error creating service discovery watcher", "service", proc.Service, "err", err)
			return err
		}
		defer stream.Close()

	outer:
		for {
			select {
			case event, ok := <-events:
				if !ok {
					log.Error("error creating service discovery watcher, channel closed", "service", proc.Service)
					return fmt.Errorf("deployer: could not create watcher for service: %s", proc.Service)
				}
				if event.Kind == discoverd.EventKindCurrent {
					break outer
				}
			case <-time.After(5 * time.Second):
				log.Error("error creating service discovery watcher, timeout reached", "service", proc.Service)
				return fmt.Errorf("deployer: could not create watcher for service: %s", proc.Service)
			}
		}
		go func() {
			for {
				event, ok := <-events
				if !ok {
					// if this happens, it means defer cleanup is in progress

					// TODO: this could also happen if the stream connection
					// dropped. handle that case
					return
				}
				deploy.serviceEvents <- event
			}
		}()
	}

	if len(deploy.useJobEvents) > 0 {
		log.Info("getting job event stream")
		events := make(chan *ct.JobEvent)
		stream, err := client.StreamJobEvents(d.AppID, 0, events)
		if err != nil {
			log.Error("error getting job event stream", "err", err)
			return err
		}
		defer stream.Close()
		deploy.jobEvents = events
	}

	return performFunc(deploy)
}

type jobEvents map[string]map[string]int

// TODO: share with tests
func jobEventsEqual(expected, actual jobEvents) bool {
	for typ, events := range expected {
		diff, ok := actual[typ]
		if !ok {
			return false
		}
		for state, count := range events {
			if diff[state] != count {
				return false
			}
		}
	}
	return true
}

func (d *Deploy) waitForJobEvents(releaseID string, expected jobEvents, log log15.Logger) error {
	actual := make(jobEvents)

	type jobIDState struct{ jobID, state string }
	sentEvents := make(map[jobIDState]struct{})

	handleEvent := func(jobID, typ, state string) {
		// don't send duplicate events
		if _, ok := sentEvents[jobIDState{jobID, state}]; ok {
			return
		}
		sentEvents[jobIDState{jobID, state}] = struct{}{}

		if _, ok := actual[typ]; !ok {
			actual[typ] = make(map[string]int)
		}
		actual[typ][state] += 1
		d.deployEvents <- ct.DeploymentEvent{
			ReleaseID: releaseID,
			JobState:  state,
			JobType:   typ,
		}
	}

	for {
		select {
		case event := <-d.serviceEvents:
			if id, ok := event.Instance.Meta["FLYNN_APP_ID"]; !ok || id != d.AppID {
				continue
			}
			if id, ok := event.Instance.Meta["FLYNN_RELEASE_ID"]; !ok || id != releaseID {
				continue
			}
			typ, ok := event.Instance.Meta["FLYNN_PROCESS_TYPE"]
			if !ok {
				continue
			}
			if _, ok := d.useJobEvents[typ]; ok {
				continue
			}
			jobID, ok := event.Instance.Meta["FLYNN_JOB_ID"]
			if !ok {
				continue
			}
			log.Info("got service event", "job_id", jobID, "type", typ, "state", event.Kind)
			if event.Kind == discoverd.EventKindUp {
				handleEvent(jobID, typ, "up")
			}
			if jobEventsEqual(expected, actual) {
				return nil
			}
		case event, ok := <-d.jobEvents:
			if !ok {
				// if this happens, it means defer cleanup is in progress

				// TODO: this could also happen if the stream connection
				// dropped. handle that case
				return nil
			}
			if event.Job.ReleaseID != releaseID {
				continue
			}

			// if service discovery is being used for the job's type, ignore up events and fail
			// the deployment if we get a down event when waiting for the job to come up.
			if _, ok := d.useJobEvents[event.Type]; !ok {
				if event.State == "up" {
					continue
				}
				if expected[event.Type]["up"] > 0 && event.IsDown() {
					handleEvent(event.JobID, event.Type, "down")
					return fmt.Errorf("%s process type failed to start, got %s job event", event.Type, event.State)
				}
			}

			log.Info("got job event", "job_id", event.JobID, "type", event.Type, "state", event.State)
			if _, ok := actual[event.Type]; !ok {
				actual[event.Type] = make(map[string]int)
			}
			switch event.State {
			case "up":
				handleEvent(event.JobID, event.Type, "up")
			case "down", "crashed":
				handleEvent(event.JobID, event.Type, "down")
			case "failed":
				handleEvent(event.JobID, event.Type, "failed")
				return fmt.Errorf("deployer: %s job failed to start", event.Type)
			}
			if jobEventsEqual(expected, actual) {
				return nil
			}
		case <-time.After(60 * time.Second):
			return fmt.Errorf("timed out waiting for job events: %v", expected)
		}
	}
}

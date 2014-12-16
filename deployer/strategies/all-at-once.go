package strategy

import (
	"github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/deployer/types"
)

func allAtOnce(client *controller.Client, d *deployer.Deployment, events chan<- deployer.DeploymentEvent) error {
	jobStream := make(chan *ct.JobEvent)
	stream, err := client.StreamJobEvents(d.AppID, 0, jobStream)
	if err != nil {
		return err
	}
	defer stream.Close()

	f, err := client.GetFormation(d.AppID, d.OldReleaseID)
	if err != nil {
		return err
	}

	if err := client.PutFormation(&ct.Formation{
		AppID:     d.AppID,
		ReleaseID: d.NewReleaseID,
		Processes: f.Processes,
	}); err != nil {
		return err
	}
	expect := make(jobEvents)
	for typ, n := range f.Processes {
		expect[typ] = map[string]int{"up": n}
	}
	if _, _, err := waitForJobEvents(jobStream, expect); err != nil {
		return err
	}
	// scale to 0
	if err := client.PutFormation(&ct.Formation{
		AppID:     d.AppID,
		ReleaseID: d.OldReleaseID,
	}); err != nil {
		return err
	}
	expect = make(jobEvents)
	for typ, n := range f.Processes {
		expect[typ] = map[string]int{"down": n}
	}
	if _, _, err := waitForJobEvents(jobStream, expect); err != nil {
		return err
	}
	return nil
}

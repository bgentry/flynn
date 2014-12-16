package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/bgentry/que-go"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-sql"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/pq"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/jackc/pgx"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/julienschmidt/httprouter"
	"github.com/flynn/flynn/controller/client"
	"github.com/flynn/flynn/deployer/strategies"
	"github.com/flynn/flynn/deployer/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/postgres"
	"github.com/flynn/flynn/pkg/random"
	"github.com/flynn/flynn/pkg/shutdown"
)

var q *que.Client
var db *postgres.DB
var client *controller.Client

var ErrNotFound = errors.New("deployer: resource not found")

type deployID struct {
	ID string
}

func main() {
	var err error
	client, err = controller.NewClient("", os.Getenv("CONTROLLER_AUTH_KEY"))
	if err != nil {
		log.Fatalln("Unable to create controller client:", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "4000"
	}

	addr := ":" + port
	if err := discoverd.Register("flynn-deployer", addr); err != nil {
		log.Fatal(err)
	}

	postgres.Wait("")
	db, err = postgres.Open("", "")
	if err != nil {
		log.Fatal(err)
	}

	if err := migrateDB(db.DB); err != nil {
		log.Fatal(err)
	}

	pgxcfg, err := pgx.ParseURI(fmt.Sprintf("http://%s:%s@%s/%s", os.Getenv("PGUSER"), os.Getenv("PGPASSWORD"), db.Addr(), os.Getenv("PGDATABASE")))
	if err != nil {
		log.Fatal(err)
	}

	pgxpool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:   pgxcfg,
		AfterConnect: que.PrepareStatements,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pgxpool.Close()

	q = que.NewClient(pgxpool)
	wm := que.WorkMap{
		"Deployment": handleJob,
	}

	workers := que.NewWorkerPool(q, wm, 10)
	go workers.Start()
	shutdown.BeforeExit(func() { workers.Shutdown() })

	router := httprouter.New()
	router.POST("/deployments", createDeployment)
	router.GET("/deployments/:deployment_id/events", streamDeploymentEvents)

	log.Println("Listening for HTTP requests on", addr)
	log.Fatal(http.ListenAndServe(addr, router))
}

func handleJob(job *que.Job) (e error) {
	var args deployID
	if err := json.Unmarshal(job.Args, &args); err != nil {
		// TODO: log error
		return err
	}
	id := args.ID
	deployment, err := getDeployment(id)
	if err != nil {
		// TODO: log/handle error
		return nil
	}
	strategyFunc, err := strategy.Get(deployment.Strategy)
	if err != nil {
		// TODO: log/handle error
		return nil
	}
	events := make(chan deployer.DeploymentEvent)
	defer close(events)
	go func() {
		for e := range events {
			sendDeploymentEvent(e)
		}
	}()
	if err := strategyFunc(client, deployment, events); err != nil {
		// TODO: log/handle error
		return nil
	}
	if err := client.SetAppRelease(deployment.AppID, deployment.NewReleaseID); err != nil {
		return err
	}
	return nil
}

func createDeployment(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	deployment := &deployer.Deployment{}
	if err := json.NewDecoder(req.Body).Decode(deployment); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if _, err := client.GetApp(deployment.AppID); err != nil {
		if err == controller.ErrNotFound {
			http.Error(w, fmt.Sprintf(`no app with id "%s"`, deployment.AppID), 400)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}
	if _, err := client.GetRelease(deployment.OldReleaseID); err != nil {
		if err == controller.ErrNotFound {
			http.Error(w, fmt.Sprintf(`no release with id "%s"`, deployment.OldReleaseID), 400)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}
	if _, err := client.GetRelease(deployment.NewReleaseID); err != nil {
		if err == controller.ErrNotFound {
			http.Error(w, fmt.Sprintf(`no release with id "%s"`, deployment.NewReleaseID), 400)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}
	if deployment.ID == "" {
		deployment.ID = random.UUID()
	}
	// TODO: wrap insert + queue push in a transaction
	query := "INSERT INTO deployments (deployment_id, app_id, old_release_id, new_release_id, strategy) VALUES ($1, $2, $3, $4, $5) RETURNING created_at"
	if err := db.QueryRow(query, deployment.ID, deployment.AppID, deployment.OldReleaseID, deployment.NewReleaseID, deployment.Strategy).Scan(&deployment.CreatedAt); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	args, err := json.Marshal(deployID{ID: deployment.ID})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if err := q.Enqueue(&que.Job{
		Type: "Deployment",
		Args: args,
	}); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	deployment.ID = postgres.CleanUUID(deployment.ID)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(deployment); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func getDeployment(id string) (*deployer.Deployment, error) {
	d := &deployer.Deployment{}
	query := "SELECT deployment_id, app_id, old_release_id, new_release_id, strategy, created_at FROM deployments WHERE deployment_id = $1"
	err := db.QueryRow(query, id).Scan(&d.ID, &d.AppID, &d.OldReleaseID, &d.NewReleaseID, &d.Strategy, &d.CreatedAt)
	d.ID = postgres.CleanUUID(d.ID)
	d.OldReleaseID = postgres.CleanUUID(d.OldReleaseID)
	d.NewReleaseID = postgres.CleanUUID(d.NewReleaseID)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func sendDeploymentEvent(e deployer.DeploymentEvent) error {
	query := "INSERT INTO deployment_events (deployment_id, release_id, job_type, job_state) VALUES ($1, $2, $3, $4)"
	return db.Exec(query, e.DeploymentID, e.ReleaseID, e.JobType, e.JobState)
}

// TODO: share with controller streamJobs
func streamDeploymentEvents(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	deploymentID := params.ByName("deployment_id")
	var lastID int64
	lastIDHeader := req.Header.Get("Last-Event-Id")
	if lastIDHeader != "" {
		var err error
		lastID, err = strconv.ParseInt(lastIDHeader, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf(`invalid Last-Event-Id header "%s"`, lastIDHeader), 400)
		}
	}

	var err error
	defer func() {
		if err != nil {
			http.Error(w, err.Error(), 500)
		}
	}()

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")

	sendKeepAlive := func() error {
		if _, err := w.Write([]byte(":\n")); err != nil {
			return err
		}
		w.(http.Flusher).Flush()
		return nil
	}

	sendDeploymentEvent := func(e *deployer.DeploymentEvent) error {
		if _, err := fmt.Fprintf(w, "id: %d\ndata: ", e.ID); err != nil {
			return err
		}
		if err := json.NewEncoder(w).Encode(e); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
		w.(http.Flusher).Flush()
		return nil
	}

	connected := make(chan struct{})
	done := make(chan struct{})
	listenEvent := func(ev pq.ListenerEventType, listenErr error) {
		switch ev {
		case pq.ListenerEventConnected:
			close(connected)
		case pq.ListenerEventDisconnected:
			close(done)
		case pq.ListenerEventConnectionAttemptFailed:
			err = listenErr
			close(done)
		}
	}
	listener := pq.NewListener(db.DSN(), 10*time.Second, time.Minute, listenEvent)
	defer listener.Close()
	listener.Listen("deployment_events:" + postgres.FormatUUID(deploymentID))

	var currID int64
	if lastID >= 0 {
		events, err := listDeploymentEvents(deploymentID, lastID)
		if err != nil {
			return
		}
		for _, e := range events {
			if err = sendDeploymentEvent(e); err != nil {
				return
			}
			currID = e.ID
		}
	}

	select {
	case <-done:
		return
	case <-connected:
	}

	if err = sendKeepAlive(); err != nil {
		return
	}

	closed := w.(http.CloseNotifier).CloseNotify()
	for {
		select {
		case <-done:
			return
		case <-closed:
			return
		case <-time.After(30 * time.Second):
			if err = sendKeepAlive(); err != nil {
				return
			}
		case n := <-listener.Notify:
			id, err := strconv.ParseInt(n.Extra, 10, 64)
			if err != nil {
				return
			}
			if id <= currID {
				continue
			}
			e, err := getDeploymentEvent(id)
			if err != nil {
				return
			}
			if err = sendDeploymentEvent(e); err != nil {
				return
			}
		}
	}
}

func listDeploymentEvents(deploymentID string, sinceID int64) ([]*deployer.DeploymentEvent, error) {
	query := "SELECT event_id, deployment_id, release_id, job_type, job_state, created_at FROM deployment_events WHERE deployment_id = $1 AND event_id > $2"
	rows, err := db.Query(query, deploymentID, sinceID)
	if err != nil {
		return nil, err
	}
	var events []*deployer.DeploymentEvent
	for rows.Next() {
		event, err := scanDeploymentEvent(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func getDeploymentEvent(id int64) (*deployer.DeploymentEvent, error) {
	row := db.QueryRow("SELECT event_id, deployment_id, release_id, job_type, job_state, created_at FROM deployment_events WHERE event_id = $1", id)
	return scanDeploymentEvent(row)
}

func scanDeploymentEvent(s postgres.Scanner) (*deployer.DeploymentEvent, error) {
	event := &deployer.DeploymentEvent{}
	err := s.Scan(&event.ID, &event.DeploymentID, &event.ReleaseID, &event.JobType, &event.JobState, &event.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			err = ErrNotFound
		}
		return nil, err
	}
	event.DeploymentID = postgres.CleanUUID(event.DeploymentID)
	event.ReleaseID = postgres.CleanUUID(event.ReleaseID)
	return event, nil
}

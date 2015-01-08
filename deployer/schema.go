package main

import (
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-sql"
	"github.com/flynn/flynn/pkg/postgres"
)

func migrateDB(db *sql.DB) error {
	m := postgres.NewMigrations()
	m.Add(1,
		`CREATE TABLE que_jobs (
    priority    smallint    NOT NULL DEFAULT 100,
    run_at      timestamptz NOT NULL DEFAULT now(),
    job_id      bigserial   NOT NULL,
    job_class   text        NOT NULL,
    args        json        NOT NULL DEFAULT '[]'::json,
    error_count integer     NOT NULL DEFAULT 0,
    last_error  text,
    queue       text        NOT NULL DEFAULT '',

    CONSTRAINT que_jobs_pkey PRIMARY KEY (queue, priority, run_at, job_id))`,
		`COMMENT ON TABLE que_jobs IS '3'`,
	)
	m.Add(2,
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,

		`CREATE TYPE deployment_strategy AS ENUM ('all-at-once', 'one-by-one')`,
		`CREATE TABLE deployments (
    deployment_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    app_id uuid NOT NULL,
    old_release_id uuid NOT NULL,
    new_release_id uuid NOT NULL,
    strategy deployment_strategy NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now())`,

		`CREATE SEQUENCE deployment_event_ids`,
		`CREATE TYPE deployment_status AS ENUM ('running', 'complete', 'failed')`,
		`CREATE TABLE deployment_events (
    event_id bigint PRIMARY KEY DEFAULT nextval('deployment_event_ids'),
    deployment_id uuid NOT NULL REFERENCES deployments (deployment_id),
    release_id uuid NOT NULL,
    status deployment_status NOT NULL DEFAULT 'running',
    job_type text,
    job_state text,
    created_at timestamptz NOT NULL DEFAULT now())`,

		`CREATE FUNCTION notify_deployment_event() RETURNS TRIGGER AS $$
    BEGIN
    PERFORM pg_notify('deployment_events:' || NEW.deployment_id, NEW.event_id || '');
        RETURN NULL;
    END;
$$ LANGUAGE plpgsql`,

		`CREATE TRIGGER notify_deployment_event
    AFTER INSERT ON deployment_events
    FOR EACH ROW EXECUTE PROCEDURE notify_deployment_event()`,
	)
	return m.Migrate(db)
}

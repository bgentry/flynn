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
	return m.Migrate(db)
}

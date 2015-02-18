package main

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-sql"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/jackc/pgx"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/discoverd/testutil"
	"github.com/flynn/flynn/discoverd/testutil/etcdrunner"
	"github.com/flynn/flynn/pkg/testutils/postgres"
	"github.com/flynn/flynn/router/types"
)

func init() {
	testMode = true
}

type discoverdClient interface {
	DiscoverdClient
	AddServiceAndRegister(string, string) (discoverd.Heartbeater, error)
}

// discoverdWrapper wraps a discoverd client to expose Close method that closes
// all heartbeaters
type discoverdWrapper struct {
	discoverdClient
	hbs []io.Closer
}

func (d *discoverdWrapper) AddServiceAndRegister(service, addr string) (discoverd.Heartbeater, error) {
	hb, err := d.discoverdClient.AddServiceAndRegister(service, addr)
	if err != nil {
		return nil, err
	}
	d.hbs = append(d.hbs, hb)
	return hb, nil
}

func (d *discoverdWrapper) Cleanup() {
	for _, hb := range d.hbs {
		hb.Close()
	}
	d.hbs = nil
}

func setup(t etcdrunner.TestingT) (*discoverdWrapper, func()) {
	etcdAddr, killEtcd := etcdrunner.RunEtcdServer(t)
	dc, killDiscoverd := testutil.BootDiscoverd(t, "", etcdAddr)
	dw := &discoverdWrapper{discoverdClient: dc}

	return dw, func() {
		killDiscoverd()
		killEtcd()
	}
}

// Hook gocheck up to the "go test" runner
func Test(t *testing.T) { TestingT(t) }

type S struct {
	discoverd *discoverdWrapper
	cleanup   func()
	pgx       *pgx.ConnPool
}

var _ = Suite(&S{})

const dbname = "routertest"

func (s *S) SetUpSuite(c *C) {
	s.discoverd, s.cleanup = setup(c)

	if err := pgtestutils.SetupPostgres(dbname); err != nil {
		c.Fatal(err)
	}

	dsn := fmt.Sprintf("dbname=%s", dbname)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		c.Fatal(err)
	}
	if err = migrateDB(db); err != nil {
		c.Fatal(err)
	}
	db.Close()
	pgxpool, err := pgx.NewConnPool(newPgxConnPoolConfig())
	if err != nil {
		c.Fatal(err)
	}
	s.pgx = pgxpool
	s.pgx.Exec(sqlCreateTruncateTables)
}

func newPgxConnPoolConfig() pgx.ConnPoolConfig {
	return pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     os.Getenv("PGHOST"),
			Database: dbname,
		},
	}
}
func (s *S) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *S) TearDownTest(c *C) {
	s.discoverd.Cleanup()
	s.pgx.Exec("SELECT truncate_tables()")
}

const waitTimeout = time.Second

func waitForEvent(c *C, w Watcher, event string, id string) func() *router.Event {
	ch := make(chan *router.Event)
	w.Watch(ch)
	return func() *router.Event {
		defer w.Unwatch(ch)
		start := time.Now()
		for {
			timeout := waitTimeout - time.Now().Sub(start)
			if timeout <= 0 {
				break
			}
			select {
			case e := <-ch:
				if e.Event == event && (id == "" || e.ID == id) {
					return e
				}
			case <-time.After(timeout):
				break
			}
		}
		c.Fatalf("timeout exceeded waiting for %s %s", event, id)
		return nil
	}
}

func discoverdRegisterTCP(c *C, l *TCPListener, addr string) func() {
	return discoverdRegisterTCPService(c, l, "test", addr)
}

func discoverdRegisterTCPService(c *C, l *TCPListener, name, addr string) func() {
	dc := l.discoverd.(discoverdClient)
	sc := l.services[name].sc
	return discoverdRegister(c, dc, sc.(*discoverdServiceCache), name, addr)
}

func discoverdRegisterHTTP(c *C, l *HTTPListener, addr string) func() {
	return discoverdRegisterHTTPService(c, l, "test", addr)
}

func discoverdRegisterHTTPService(c *C, l *HTTPListener, name, addr string) func() {
	dc := l.discoverd.(discoverdClient)
	sc := l.services[name].sc
	return discoverdRegister(c, dc, sc.(*discoverdServiceCache), name, addr)
}

func discoverdRegister(c *C, dc discoverdClient, sc *discoverdServiceCache, name, addr string) func() {
	done := make(chan struct{})
	go func() {
		events := sc.watch(true)
		defer sc.unwatch(events)
		for event := range events {
			if event.Kind == discoverd.EventKindUp && event.Instance.Addr == addr {
				close(done)
				return
			}
		}
	}()
	hb, err := dc.AddServiceAndRegister(name, addr)
	c.Assert(err, IsNil)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for discoverd registration")
	}
	return discoverdUnregisterFunc(c, hb, sc)
}

func discoverdUnregisterFunc(c *C, hb discoverd.Heartbeater, sc *discoverdServiceCache) func() {
	return func() {
		done := make(chan struct{})
		started := make(chan struct{})
		go func() {
			events := sc.watch(false)
			defer sc.unwatch(events)
			close(started)
			for event := range events {
				if event.Kind == discoverd.EventKindDown && event.Instance.Addr == hb.Addr() {
					close(done)
					return
				}
			}
		}()
		<-started
		c.Assert(hb.Close(), IsNil)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			c.Fatal("timed out waiting for discoverd unregister")
		}
	}
}

func addRoute(c *C, l Listener, r *router.Route) *router.Route {
	wait := waitForEvent(c, l, "set", "")
	err := l.AddRoute(r)
	c.Assert(err, IsNil)
	wait()
	return r
}

const sqlCreateTruncateTables = `
CREATE OR REPLACE FUNCTION truncate_tables() RETURNS void AS $$
DECLARE
    statements CURSOR FOR
        SELECT tablename FROM pg_tables
        WHERE tablename != 'schema_migrations'
          AND tableowner = session_user
          AND schemaname = 'public';
BEGIN
    FOR stmt IN statements LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(stmt.tablename) || ' CASCADE;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
`

func removeRoute(c *C, l Listener, id string) {
	wait := waitForEvent(c, l, "remove", "")
	err := l.RemoveRoute(id)
	c.Assert(err, IsNil)
	wait()
}

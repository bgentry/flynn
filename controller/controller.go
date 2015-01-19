package main

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/go-martini/martini"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/julienschmidt/httprouter"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/martini-contrib/binding"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/martini-contrib/render"
	"github.com/flynn/flynn/controller/name"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/cluster"
	"github.com/flynn/flynn/pkg/cors"
	"github.com/flynn/flynn/pkg/httphelper"
	"github.com/flynn/flynn/pkg/postgres"
	"github.com/flynn/flynn/pkg/resource"
	"github.com/flynn/flynn/pkg/shutdown"
	routerc "github.com/flynn/flynn/router/client"
	"github.com/flynn/flynn/router/types"
)

var ErrNotFound = errors.New("controller: resource not found")

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	addr := ":" + port

	if seed := os.Getenv("NAME_SEED"); seed != "" {
		s, err := hex.DecodeString(seed)
		if err != nil {
			log.Fatalln("error decoding NAME_SEED:", err)
		}
		name.SetSeed(s)
	}

	postgres.Wait("")
	db, err := postgres.Open("", "")
	if err != nil {
		log.Fatal(err)
	}

	if err := migrateDB(db.DB); err != nil {
		log.Fatal(err)
	}

	cc, err := cluster.NewClient()
	if err != nil {
		log.Fatal(err)
	}

	sc, err := routerc.New()
	if err != nil {
		log.Fatal(err)
	}

	if err := discoverd.Register("flynn-controller", addr); err != nil {
		log.Fatal(err)
	}

	shutdown.BeforeExit(func() {
		discoverd.Unregister("flynn-controller", addr)
	})

	handler, _ := appHandler(handlerConfig{db: db, cc: cc, sc: sc, dc: discoverd.DefaultClient, key: os.Getenv("AUTH_KEY")})
	log.Fatal(http.ListenAndServe(addr, handler))
}

type handlerConfig struct {
	db  *postgres.DB
	cc  clusterClient
	sc  routerc.Client
	dc  *discoverd.Client
	key string
}

type ResponseHelper interface {
	Error(error)
	JSON(int, interface{})
	WriteHeader(int)
}

type responseHelper struct {
	http.ResponseWriter
	render.Render
}

// deprecated, use respondWithError
func (r *responseHelper) Error(err error) {
	switch err.(type) {
	case ct.ValidationError:
		r.JSON(400, err)
	case *json.SyntaxError, *json.UnmarshalTypeError:
		r.JSON(400, ct.ValidationError{Message: "The provided JSON input is invalid"})
	default:
		if err == ErrNotFound {
			r.WriteHeader(404)
			return
		}
		log.Println(err)
		r.JSON(500, struct{}{})
	}
}

// NOTE: this is temporary until httphelper supports custom errors
func respondWithError(w http.ResponseWriter, err error) {
	switch err.(type) {
	case ct.ValidationError:
		httphelper.JSON(w, 400, err)
	default:
		if err == ErrNotFound {
			w.WriteHeader(404)
			return
		}
		httphelper.Error(w, err)
	}
}

func responseHelperHandler(c martini.Context, w http.ResponseWriter, r render.Render) {
	c.MapTo(&responseHelper{w, r}, (*ResponseHelper)(nil))
}

func appHandler(c handlerConfig) (http.Handler, *martini.Martini) {
	r := martini.NewRouter()
	m := martini.New()
	m.Map(log.New(os.Stdout, "[controller] ", log.LstdFlags|log.Lmicroseconds))
	m.Use(martini.Logger())
	m.Use(martini.Recovery())
	m.Use(render.Renderer())
	m.Use(responseHelperHandler)
	m.Action(r.Handle)

	providerRepo := NewProviderRepo(c.db)
	keyRepo := NewKeyRepo(c.db)
	resourceRepo := NewResourceRepo(c.db)
	appRepo := NewAppRepo(c.db, os.Getenv("DEFAULT_ROUTE_DOMAIN"), c.sc)
	artifactRepo := NewArtifactRepo(c.db)
	releaseRepo := NewReleaseRepo(c.db)
	jobRepo := NewJobRepo(c.db)
	formationRepo := NewFormationRepo(c.db, appRepo, releaseRepo, artifactRepo)
	m.Map(resourceRepo)
	m.Map(appRepo)
	m.Map(artifactRepo)
	m.Map(releaseRepo)
	m.Map(jobRepo)
	m.Map(formationRepo)
	m.Map(c.dc)
	m.MapTo(c.cc, (*clusterClient)(nil))
	m.MapTo(c.sc, (*routerc.Client)(nil))
	m.MapTo(c.dc, (*resource.DiscoverdClient)(nil))

	// We're transitioning away from martini to httprouter
	httpRouter := httprouter.New()
	httpRouter.NotFound = http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		m.ServeHTTP(res, req)
	})

	_getApp := crud(httpRouter, "apps", ct.App{}, appRepo)
	_getRelease := crud(httpRouter, "releases", ct.Release{}, releaseRepo)
	getProvider := crud(httpRouter, "providers", ct.Provider{}, providerRepo)
	crud(httpRouter, "artifacts", ct.Artifact{}, artifactRepo)
	crud(httpRouter, "keys", ct.Key{}, keyRepo)

	getApp := func(params httprouter.Params) (*ct.App, error) {
		_app, err := _getApp(params)
		if err != nil {
			return nil, err
		}
		app, _ := _app.(*ct.App)
		return app, nil
	}

	getRelease := func(params httprouter.Params) (*ct.Release, error) {
		_release, err := _getRelease(params)
		if err != nil {
			return nil, err
		}
		release, _ := _release.(*ct.Release)
		return release, nil
	}

	httpRouter.PUT("/apps/:apps_id/formations/:releases_id", func(res http.ResponseWriter, req *http.Request, params httprouter.Params) {
		app, err := getApp(params)
		if err != nil {
			respondWithError(res, err)
			return
		}
		release, err := getRelease(params)
		if err != nil {
			respondWithError(res, err)
			return
		}

		putFormation(formationRepo, app, release, req, res)
	})

	httpRouter.GET("/apps/:apps_id/formations/:releases_id", func(res http.ResponseWriter, req *http.Request, params httprouter.Params) {
		app, err := getApp(params)
		if err != nil {
			respondWithError(res, err)
			return
		}
		formation, err := formationRepo.Get(app.ID, params.ByName("releases_id"))
		if err != nil {
			respondWithError(res, err)
			return
		}
		httphelper.JSON(res, 200, formation)
	})

	httpRouter.DELETE("/apps/:apps_id/formations/:releases_id", func(res http.ResponseWriter, req *http.Request, params httprouter.Params) {
		app, err := getApp(params)
		if err != nil {
			respondWithError(res, err)
			return
		}
		formation, err := formationRepo.Get(app.ID, params.ByName("releases_id"))
		if err != nil {
			respondWithError(res, err)
			return
		}
		err = formationRepo.Remove(app.ID, formation.ReleaseID)
		if err != nil {
			respondWithError(res, err)
			return
		}
		res.WriteHeader(200)
	})

	httpRouter.GET("/apps/:apps_id/formations", func(res http.ResponseWriter, req *http.Request, params httprouter.Params) {
		app, err := getApp(params)
		if err != nil {
			respondWithError(res, err)
			return
		}
		list, err := formationRepo.List(app.ID)
		if err != nil {
			respondWithError(res, err)
			return
		}
		httphelper.JSON(res, 200, list)
	})

	// temporary
	getAppMiddleware := func(c martini.Context, params martini.Params, req *http.Request, r ResponseHelper) {
		thing, err := getApp(httprouter.Params{httprouter.Param{"apps_id", params["apps_id"]}})
		if err != nil {
			r.Error(err)
			return
		}
		c.Map(thing)
	}

	// temporary
	getProviderMiddleware := func(c martini.Context, params martini.Params, req *http.Request, r ResponseHelper) {
		thing, err := getProvider(httprouter.Params{httprouter.Param{"providers_id", params["providers_id"]}})
		if err != nil {
			r.Error(err)
			return
		}
		c.Map(thing)
	}

	r.Post("/apps/:apps_id/jobs", getAppMiddleware, binding.Bind(ct.NewJob{}), runJob)
	r.Get("/apps/:apps_id/jobs/:jobs_id", getAppMiddleware, getJob)
	r.Put("/apps/:apps_id/jobs/:jobs_id", getAppMiddleware, binding.Bind(ct.Job{}), putJob)
	r.Get("/apps/:apps_id/jobs", getAppMiddleware, listJobs)
	r.Delete("/apps/:apps_id/jobs/:jobs_id", getAppMiddleware, connectHostMiddleware, killJob)
	r.Get("/apps/:apps_id/jobs/:jobs_id/log", getAppMiddleware, connectHostMiddleware, jobLog)

	r.Put("/apps/:apps_id/release", getAppMiddleware, binding.Bind(releaseID{}), setAppRelease)
	r.Get("/apps/:apps_id/release", getAppMiddleware, getAppRelease)

	r.Post("/providers/:providers_id/resources", getProviderMiddleware, binding.Bind(ct.ResourceReq{}), resourceServerMiddleware, provisionResource)
	r.Get("/providers/:providers_id/resources", getProviderMiddleware, getProviderResources)
	r.Get("/providers/:providers_id/resources/:resources_id", getProviderMiddleware, getResourceMiddleware, getResource)
	r.Put("/providers/:providers_id/resources/:resources_id", getProviderMiddleware, binding.Bind(ct.Resource{}), putResource)
	r.Get("/apps/:apps_id/resources", getAppMiddleware, getAppResources)

	r.Post("/apps/:apps_id/routes", getAppMiddleware, binding.Bind(router.Route{}), createRoute)
	r.Get("/apps/:apps_id/routes", getAppMiddleware, getRouteList)
	r.Get("/apps/:apps_id/routes/:routes_type/:routes_id", getAppMiddleware, getRouteMiddleware, getRoute)
	r.Delete("/apps/:apps_id/routes/:routes_type/:routes_id", getAppMiddleware, getRouteMiddleware, deleteRoute)

	r.Get("/formations", getFormations)

	return muxHandler(httpRouter, c.key), m
}

func muxHandler(main http.Handler, authKey string) http.Handler {
	corsHandler := cors.Allow(&cors.Options{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
		AllowHeaders:     []string{"Authorization", "Accept", "Content-Type", "If-Match", "If-None-Match"},
		ExposeHeaders:    []string{"ETag"},
		AllowCredentials: true,
		MaxAge:           time.Hour,
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corsHandler(w, r)
		if r.URL.Path == "/ping" || r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		_, password, _ := parseBasicAuth(r.Header)
		if password == "" && strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			password = r.URL.Query().Get("key")
		}
		if len(password) != len(authKey) || subtle.ConstantTimeCompare([]byte(password), []byte(authKey)) != 1 {
			w.WriteHeader(401)
			return
		}
		main.ServeHTTP(w, r)
	})
}

func putFormation(repo *FormationRepo, app *ct.App, release *ct.Release, req *http.Request, res http.ResponseWriter) {
	var formation ct.Formation
	dec := json.NewDecoder(req.Body)
	err := dec.Decode(&formation)
	if err != nil {
		respondWithError(res, err)
		return
	}

	formation.AppID = app.ID
	formation.ReleaseID = release.ID
	if app.Protected {
		for typ := range release.Processes {
			if formation.Processes[typ] == 0 {
				respondWithError(res, ct.ValidationError{Message: "unable to scale to zero, app is protected"})
				return
			}
		}
	}
	if err := repo.Add(&formation); err != nil {
		respondWithError(res, err)
		return
	}
	httphelper.JSON(res, 200, &formation)
}

type releaseID struct {
	ID string `json:"id"`
}

func setAppRelease(app *ct.App, rid releaseID, apps *AppRepo, releases *ReleaseRepo, formations *FormationRepo, r ResponseHelper) {
	rel, err := releases.Get(rid.ID)
	if err != nil {
		if err == ErrNotFound {
			err = ct.ValidationError{
				Message: fmt.Sprintf("could not find release with ID %s", rid.ID),
			}
		}
		r.Error(err)
		return
	}
	release := rel.(*ct.Release)
	apps.SetRelease(app.ID, release.ID)

	// TODO: use transaction/lock
	fs, err := formations.List(app.ID)
	if err != nil {
		r.Error(err)
		return
	}
	if len(fs) == 1 && fs[0].ReleaseID != release.ID {
		if err := formations.Add(&ct.Formation{
			AppID:     app.ID,
			ReleaseID: release.ID,
			Processes: fs[0].Processes,
		}); err != nil {
			r.Error(err)
			return
		}
		if err := formations.Remove(app.ID, fs[0].ReleaseID); err != nil {
			r.Error(err)
			return
		}
	}

	r.JSON(200, release)
}

func getAppRelease(app *ct.App, apps *AppRepo, r ResponseHelper) {
	release, err := apps.GetRelease(app.ID)
	if err != nil {
		r.Error(err)
		return
	}
	r.JSON(200, release)
}

func resourceServerMiddleware(c martini.Context, p *ct.Provider, dc resource.DiscoverdClient, r ResponseHelper) {
	server, err := resource.NewServerWithDiscoverd(p.URL, dc)
	if err != nil {
		r.Error(err)
		return
	}
	c.Map(server)
	c.Next()
	server.Close()
}

func putResource(p *ct.Provider, params martini.Params, resource ct.Resource, repo *ResourceRepo, r ResponseHelper) {
	resource.ID = params["resources_id"]
	resource.ProviderID = p.ID
	if err := repo.Add(&resource); err != nil {
		r.Error(err)
		return
	}
	r.JSON(200, &resource)
}

func provisionResource(rs *resource.Server, p *ct.Provider, req ct.ResourceReq, repo *ResourceRepo, r ResponseHelper) {
	var config []byte
	if req.Config != nil {
		config = *req.Config
	} else {
		config = []byte(`{}`)
	}
	data, err := rs.Provision(config)
	if err != nil {
		r.Error(err)
		return
	}

	res := &ct.Resource{
		ProviderID: p.ID,
		ExternalID: data.ID,
		Env:        data.Env,
		Apps:       req.Apps,
	}
	if err := repo.Add(res); err != nil {
		// TODO: attempt to "rollback" provisioning
		r.Error(err)
		return
	}
	r.JSON(200, res)
}

func getResourceMiddleware(c martini.Context, params martini.Params, repo *ResourceRepo, r ResponseHelper) {
	resource, err := repo.Get(params["resources_id"])
	if err != nil {
		r.Error(err)
		return
	}
	c.Map(resource)
}

func getResource(resource *ct.Resource, r ResponseHelper) {
	r.JSON(200, resource)
}

func getProviderResources(p *ct.Provider, repo *ResourceRepo, r ResponseHelper) {
	res, err := repo.ProviderList(p.ID)
	if err != nil {
		r.Error(err)
		return
	}
	r.JSON(200, res)
}

func getAppResources(app *ct.App, repo *ResourceRepo, r ResponseHelper) {
	res, err := repo.AppList(app.ID)
	if err != nil {
		r.Error(err)
		return
	}
	r.JSON(200, res)
}

func parseBasicAuth(h http.Header) (username, password string, err error) {
	s := strings.SplitN(h.Get("Authorization"), " ", 2)

	if len(s) != 2 {
		return "", "", errors.New("failed to parse authentication string ")
	}
	if s[0] != "Basic" {
		return "", "", fmt.Errorf("authorization scheme is %v, not Basic ", s[0])
	}

	c, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return "", "", errors.New("failed to parse base64 basic credentials")
	}

	s = strings.SplitN(string(c), ":", 2)
	if len(s) != 2 {
		return "", "", errors.New("failed to parse basic credentials")
	}

	return s[0], s[1], nil
}

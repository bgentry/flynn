package main

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/kavu/go_reuseport"
	"github.com/flynn/flynn/Godeps/_workspace/src/golang.org/x/crypto/nacl/secretbox"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/random"
	"github.com/flynn/flynn/pkg/tlsconfig"
	"github.com/flynn/flynn/router/types"
)

type HTTPListener struct {
	Watcher
	DataStoreReader

	Addr    string
	TLSAddr string

	mtx      sync.RWMutex
	domains  map[string]*httpRoute
	routes   map[string]*httpRoute
	services map[string]*httpService

	discoverd DiscoverdClient
	ds        DataStore
	wm        *WatchManager

	listener    net.Listener
	tlsListener net.Listener
	closed      bool
	cookieKey   *[32]byte
	keypair     tls.Certificate
}

type DiscoverdClient interface {
	NewServiceSet(string) (discoverd.ServiceSet, error)
}

func (s *HTTPListener) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, service := range s.services {
		service.ss.Close()
	}
	s.listener.Close()
	s.tlsListener.Close()
	s.ds.StopSync()
	s.closed = true
	return nil
}

func (s *HTTPListener) Start() error {
	if s.Watcher != nil {
		return errors.New("router: http listener already started")
	}
	if s.wm == nil {
		s.wm = NewWatchManager()
	}
	s.Watcher = s.wm

	if s.ds == nil {
		return errors.New("router: http listener missing data store")
	}
	s.DataStoreReader = s.ds

	s.routes = make(map[string]*httpRoute)
	s.domains = make(map[string]*httpRoute)
	s.services = make(map[string]*httpService)

	if s.cookieKey == nil {
		s.cookieKey = &[32]byte{}
	}

	started := make(chan error)

	go s.ds.Sync(&httpSyncHandler{l: s}, started)
	if err := <-started; err != nil {
		return err
	}

	go s.listenAndServe(started)
	if err := <-started; err != nil {
		s.ds.StopSync()
		return err
	}
	s.Addr = s.listener.Addr().String()

	go s.listenAndServeTLS(started)
	if err := <-started; err != nil {
		s.ds.StopSync()
		s.listener.Close()
		return err
	}
	s.TLSAddr = s.tlsListener.Addr().String()

	return nil
}

var ErrClosed = errors.New("router: listener has been closed")

func (s *HTTPListener) AddRoute(r *router.Route) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.closed {
		return ErrClosed
	}
	r.ID = md5sum(r.HTTPRoute().Domain)
	return s.ds.Add(r)
}

func (s *HTTPListener) SetRoute(r *router.Route) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.closed {
		return ErrClosed
	}
	r.ID = md5sum(r.HTTPRoute().Domain)
	return s.ds.Set(r)
}

func md5sum(data string) string {
	digest := md5.Sum([]byte(data))
	return hex.EncodeToString(digest[:])
}

func (s *HTTPListener) RemoveRoute(id string) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return s.ds.Remove(id)
}

type httpSyncHandler struct {
	l *HTTPListener
}

func (h *httpSyncHandler) Set(data *router.Route) error {
	route := data.HTTPRoute()
	r := &httpRoute{HTTPRoute: route}

	if r.TLSCert != "" && r.TLSKey != "" {
		kp, err := tls.X509KeyPair([]byte(r.TLSCert), []byte(r.TLSKey))
		if err != nil {
			return err
		}
		r.keypair = &kp
		r.TLSCert = ""
		r.TLSKey = ""
	}

	h.l.mtx.Lock()
	defer h.l.mtx.Unlock()
	if h.l.closed {
		return nil
	}

	service := h.l.services[r.Service]
	if service != nil && service.name != r.Service {
		service.refs--
		if service.refs <= 0 {
			service.ss.Close()
			delete(h.l.services, service.name)
		}
		service = nil
	}
	if service == nil {
		ss, err := h.l.discoverd.NewServiceSet(r.Service)
		if err != nil {
			return err
		}
		service = &httpService{name: r.Service, ss: ss, cookieKey: h.l.cookieKey}
		h.l.services[r.Service] = service
	}
	service.refs++
	r.service = service
	h.l.routes[data.ID] = r
	h.l.domains[strings.ToLower(r.Domain)] = r

	go h.l.wm.Send(&router.Event{Event: "set", ID: r.Domain})
	return nil
}

func (h *httpSyncHandler) Remove(id string) error {
	h.l.mtx.Lock()
	defer h.l.mtx.Unlock()
	if h.l.closed {
		return nil
	}
	r, ok := h.l.routes[id]
	if !ok {
		return ErrNotFound
	}

	r.service.refs--
	if r.service.refs <= 0 {
		r.service.ss.Close()
		delete(h.l.services, r.service.name)
	}

	delete(h.l.routes, id)
	delete(h.l.domains, r.Domain)
	go h.l.wm.Send(&router.Event{Event: "remove", ID: id})
	return nil
}

func (s *HTTPListener) listenAndServe(started chan<- error) {
	_, port, err := net.SplitHostPort(s.Addr)
	if err != nil {
		started <- err
		return
	}
	s.listener, err = reuseport.NewReusablePortListener("tcp4", s.Addr)
	started <- err
	if err != nil {
		return
	}

	server := &http.Server{
		Addr: s.Addr,
		Handler: fwdProtoHandler{
			Handler: s,
			Proto:   "http",
			Port:    port,
		},
	}

	// TODO: log error
	_ = server.Serve(s.listener)
}

var errMissingTLS = errors.New("router: route not found or TLS not configured")

func (s *HTTPListener) listenAndServeTLS(started chan<- error) {
	_, port, err := net.SplitHostPort(s.TLSAddr)
	if err != nil {
		started <- err
		return
	}
	certForHandshake := func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		r := s.findRouteForHost(hello.ServerName)
		if r == nil {
			return nil, errMissingTLS
		}
		return r.keypair, nil
	}
	tlsConfig := tlsconfig.SecureCiphers(&tls.Config{
		GetCertificate: certForHandshake,
		Certificates:   []tls.Certificate{s.keypair},
	})

	server := &http.Server{
		Addr: s.TLSAddr,
		Handler: fwdProtoHandler{
			Handler: s,
			Proto:   "https",
			Port:    port,
		},
	}

	l, err := reuseport.NewReusablePortListener("tcp4", s.TLSAddr)
	started <- err
	if err != nil {
		return
	}
	s.tlsListener = tls.NewListener(l, tlsConfig)
	// TODO: log error
	_ = server.Serve(s.tlsListener)
}

func (s *HTTPListener) findRouteForHost(host string) *httpRoute {
	host = strings.ToLower(host)
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if backend, ok := s.domains[host]; ok {
		return backend
	}
	// handle wildcard domains up to 5 subdomains deep, from most-specific to
	// least-specific
	d := strings.SplitN(host, ".", 5)
	for i := len(d); i > 0; i-- {
		if backend, ok := s.domains["*."+strings.Join(d[len(d)-i:], ".")]; ok {
			return backend
		}
	}
	return nil
}

func fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Length", strconv.Itoa(len(msg)))
	w.WriteHeader(code)
	w.Write([]byte(msg))
}

const hdrUseStickySessions = "Flynn-Use-Sticky-Sessions"

func (s *HTTPListener) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r := s.findRouteForHost(req.Host)
	if r == nil {
		fail(w, 404, "Not Found")
		return
	}

	// TODO(bgentry): find a better way to access this setting in the service
	// where it's needed.
	if r.Sticky {
		req.Header.Set(hdrUseStickySessions, "true")
	}

	r.service.ServeHTTP(w, req)
}

// A domain served by a listener, associated TLS certs,
// and link to backend service set.
type httpRoute struct {
	*router.HTTPRoute

	keypair *tls.Certificate
	service *httpService
}

// A service definition: name, and set of backends.
type httpService struct {
	name string
	ss   discoverd.ServiceSet
	refs int

	cookieKey *[32]byte
}

func (s *httpService) pickBackend() string {
	addrs := s.ss.Addrs()
	if len(addrs) == 0 {
		return ""
	}
	return addrs[random.Math.Intn(len(addrs))]
}

const stickyCookie = "_backend"

func (s *httpService) pickBackendSticky(req *http.Request) (string, *http.Cookie) {
	cookie, err := req.Cookie(stickyCookie)
	if err != nil {
		return s.pickNewBackendSticky()
	}

	data, err := base64.StdEncoding.DecodeString(cookie.Value)
	if err != nil {
		return s.pickNewBackendSticky()
	}
	var nonce [24]byte
	if len(data) < len(nonce) {
		return s.pickNewBackendSticky()
	}
	copy(nonce[:], data)
	res, ok := secretbox.Open(nil, data[len(nonce):], &nonce, s.cookieKey)
	if !ok {
		return s.pickNewBackendSticky()
	}

	addr := string(res)
	ok = false
	for _, a := range s.ss.Addrs() {
		if a == addr {
			ok = true
			break
		}
	}
	if !ok {
		return s.pickNewBackendSticky()
	}

	return addr, nil
}

func (s *httpService) pickNewBackendSticky() (string, *http.Cookie) {
	backend := s.pickBackend()
	if backend == "" {
		return "", nil
	}

	var nonce [24]byte
	_, err := io.ReadFull(rand.Reader, nonce[:])
	if err != nil {
		panic(err)
	}
	out := make([]byte, len(nonce), len(nonce)+len(backend)+secretbox.Overhead)
	copy(out, nonce[:])
	out = secretbox.Seal(out, []byte(backend), &nonce, s.cookieKey)

	return backend, &http.Cookie{Name: stickyCookie, Value: base64.StdEncoding.EncodeToString(out), Path: "/"}
}

func (s *httpService) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	req.Header.Set("X-Request-Start", strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10))
	req.Header.Set("X-Request-Id", random.UUID())

	var (
		backend      string
		stickyCookie *http.Cookie
	)
	// TODO(bgentry): retry multiple backends until we get a conn, use custom
	// net.Dial func to return matchable error when no conn was made.
	if req.Header.Get(hdrUseStickySessions) == "true" {
		// TODO(bgentry): switch to better way to check sticky setting
		req.Header.Del(hdrUseStickySessions)
		backend, stickyCookie = s.pickBackendSticky(req)
	} else {
		backend = s.pickBackend()
	}

	if backend == "" {
		log.Println("no backend found")
		fail(w, 503, "Service Unavailable")
		return
	}

	// Most of this is borrowed from httputil.ReverseProxy
	outreq := new(http.Request)
	*outreq = *req // includes shallow copies of maps, but okay

	outreq.URL.Host = backend
	// Pass the Request-URI verbatim without any modifications
	outreq.URL.Opaque = strings.Split(strings.TrimPrefix(req.RequestURI, req.URL.Scheme+":"), "?")[0]
	outreq.URL.Scheme = "http"
	outreq.Proto = "HTTP/1.1"
	outreq.ProtoMajor = 1
	outreq.ProtoMinor = 1
	outreq.Close = false

	// TODO: Proxy HTTP CONNECT? (example: Go RPC over HTTP)

	// Directly bridge `Connection: Upgrade` requests
	if strings.ToLower(outreq.Header.Get("Connection")) == "upgrade" {
		s.forwardAndProxyTCP(w, outreq)
		return
	}

	// Remove hop-by-hop headers to the backend.  Especially
	// important is "Connection" because we want a persistent
	// connection, regardless of what the client sent to us.  This
	// is modifying the same underlying map from req (shallow
	// copied above) so we only copy it if necessary.
	copiedHeaders := false
	for _, h := range hopHeaders {
		if outreq.Header.Get(h) != "" {
			if !copiedHeaders {
				outreq.Header = make(http.Header)
				copyHeader(outreq.Header, req.Header)
				copiedHeaders = true
			}
			outreq.Header.Del(h)
		}
	}

	res, err := http.DefaultTransport.RoundTrip(outreq)
	if err != nil {
		log.Println("http: proxy error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer res.Body.Close()

	if stickyCookie != nil {
		res.Header.Add("Set-Cookie", stickyCookie.String())
	}

	for _, h := range hopHeaders {
		res.Header.Del(h)
	}

	copyHeader(w.Header(), res.Header)

	w.WriteHeader(res.StatusCode)
	_, err = io.Copy(w, res.Body) // TODO(bgentry): consider using a flush interval
	if err != nil {
		log.Println("reverse proxy copy err:", err)
		return
	}
}

func (s *httpService) forwardAndProxyTCP(w http.ResponseWriter, req *http.Request) {
	upconn, err := net.Dial("tcp", req.URL.Host)
	if err != nil {
		fail(w, 503, http.StatusText(503))
		return
	}
	defer upconn.Close()

	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Println("not a hijacker")
		fail(w, 500, http.StatusText(500))
		return
	}
	downconn, _, err := hj.Hijack()
	if err != nil {
		log.Println("hijack failed:", err)
		fail(w, 500, http.StatusText(500))
		return
	}
	defer downconn.Close()

	err = req.Write(upconn)
	if err != nil {
		log.Println("error copying request to target:", err)
		// TODO(bgentry): write proper 503 response for downstream
		return
	}

	errc := make(chan error, 2)
	cp := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		errc <- err
	}
	go cp(upconn, downconn)
	go cp(downconn, upconn)
	<-errc
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// Hop-by-hop headers. These are removed when sent to the backend.
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te", // canonicalized version of "TE"
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

type writeCloser interface {
	CloseWrite() error
}

func shuffle(s []string) []string {
	for i := len(s) - 1; i > 0; i-- {
		j := random.Math.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
	return s
}

// Package metrics exposes ably-server's process-wide Prometheus metrics
// (DESIGN.md §10). The metrics here are deliberately low-cardinality:
// process-wide counters, gauges, and histograms with no per-channel,
// per-connection, or per-clientId labels (those live in a separate task).
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the process-wide collectors and the registry they are
// registered against. All instrumentation methods are safe to call on a
// nil *Metrics: they no-op, so callers that were handed no Metrics (e.g.
// unit tests) need no guards.
type Metrics struct {
	registry *prometheus.Registry

	connectionsOpened  prometheus.Counter
	connectionsOpen    prometheus.Gauge
	connectionLifetime prometheus.Histogram
	attachments        prometheus.Counter
	messagesPublished  prometheus.Counter
	messagesDelivered  prometheus.Counter
	publishLatency     prometheus.Histogram
	httpRequests       *prometheus.CounterVec
}

// New builds a Metrics with its own registry (so instances are isolated
// and tests don't collide on the global default registry) and registers
// the process collectors — Go runtime and process stats plus the
// ably-server series enumerated in DESIGN.md §10.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		connectionsOpened: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_connections_opened_total",
			Help: "Total WebSocket connections opened (successful upgrades).",
		}),
		connectionsOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ably_connections_open",
			Help: "Current number of open WebSocket connections.",
		}),
		connectionLifetime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_connection_lifetime_seconds",
			Help:    "WebSocket connection lifetime in seconds, from upgrade to teardown.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		attachments: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_attachments_total",
			Help: "Total channel attachments established.",
		}),
		messagesPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_messages_published_total",
			Help: "Total inbound publishes accepted (WebSocket and REST).",
		}),
		messagesDelivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ably_messages_delivered_total",
			Help: "Total outbound MESSAGE frames forwarded to attachments.",
		}),
		publishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ably_publish_latency_seconds",
			Help:    "Latency from an inbound publish to its storage commit / ACK, in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ably_http_requests_total",
			Help: "Total HTTP requests, labelled by matched route pattern, method, and response status.",
		}, []string{"route", "method", "status"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.connectionsOpened,
		m.connectionsOpen,
		m.connectionLifetime,
		m.attachments,
		m.messagesPublished,
		m.messagesDelivered,
		m.publishLatency,
		m.httpRequests,
	)
	return m
}

// Handler returns the HTTP handler that serves the registry in the
// Prometheus text exposition format. Registered unauthenticated on the
// main listener at /metrics (DESIGN.md §10), alongside /healthz.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ConnectionOpened records a successful WebSocket upgrade: it bumps the
// opened counter and the current-open gauge.
func (m *Metrics) ConnectionOpened() {
	if m == nil {
		return
	}
	m.connectionsOpened.Inc()
	m.connectionsOpen.Inc()
}

// ConnectionClosed records a WebSocket teardown: it decrements the
// current-open gauge and observes the connection's lifetime in seconds.
func (m *Metrics) ConnectionClosed(lifetimeSeconds float64) {
	if m == nil {
		return
	}
	m.connectionsOpen.Dec()
	m.connectionLifetime.Observe(lifetimeSeconds)
}

// AttachmentOpened records a channel attachment being established.
func (m *Metrics) AttachmentOpened() {
	if m == nil {
		return
	}
	m.attachments.Inc()
}

// MessagePublished records one accepted inbound publish and observes the
// latency (in seconds) from the inbound publish to its storage commit.
func (m *Metrics) MessagePublished(latencySeconds float64) {
	if m == nil {
		return
	}
	m.messagesPublished.Inc()
	m.publishLatency.Observe(latencySeconds)
}

// MessageDelivered records one outbound MESSAGE frame forwarded to an
// attachment.
func (m *Metrics) MessageDelivered() {
	if m == nil {
		return
	}
	m.messagesDelivered.Inc()
}

// HTTPRequest records one served HTTP request against the matched route
// pattern, method, and response status.
func (m *Metrics) HTTPRequest(route, method string, status int) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
}

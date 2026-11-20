// Package metrics is a small Prometheus exporter: counters, histograms and
// gauges read at scrape time, written in the text exposition format. It
// keeps the gateway free of the Prometheus client library's dependencies.
package metrics

import (
	"bufio"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metric families in registration order.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	write(w *bufio.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.families = append(r.families, f)
}

// WriteText writes every family in the text exposition format.
func (r *Registry) WriteText(w io.Writer) error {
	r.mu.Lock()
	fams := slices.Clone(r.families)
	r.mu.Unlock()
	bw := bufio.NewWriter(w)
	for _, f := range fams {
		f.write(bw)
	}
	return bw.Flush()
}

// Handler serves the registry for scraping.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = r.WriteText(w)
	})
}

func key(values []string) string { return strings.Join(values, "\xff") }

// Counter is a family of monotonically increasing values.
type Counter struct {
	name, help string
	labels     []string

	mu     sync.Mutex
	series map[string]*counterSeries
}

type counterSeries struct {
	values []string
	v      float64
}

// NewCounter registers a counter family with the given label names.
func (r *Registry) NewCounter(name, help string, labels ...string) *Counter {
	c := &Counter{name: name, help: help, labels: labels, series: map[string]*counterSeries{}}
	r.add(c)
	return c
}

// Inc adds one to the series with these label values.
func (c *Counter) Inc(values ...string) { c.Add(1, values...) }

// Add adds v, which must not be negative, to the series with these label
// values. Missing values are recorded as empty.
func (c *Counter) Add(v float64, values ...string) {
	if v < 0 {
		return
	}
	values = fit(values, len(c.labels))
	k := key(values)
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.series[k]
	if !ok {
		s = &counterSeries{values: values}
		c.series[k] = s
	}
	s.v += v
}

// Value returns the current value of one series, for tests.
func (c *Counter) Value(values ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.series[key(fit(values, len(c.labels)))]; ok {
		return s.v
	}
	return 0
}

func (c *Counter) write(w *bufio.Writer) {
	header(w, c.name, c.help, "counter")
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range sortedKeys(c.series) {
		s := c.series[k]
		sample(w, c.name, c.labels, s.values, "", "", s.v)
	}
}

// Histogram is a family of observation distributions.
type Histogram struct {
	name, help string
	labels     []string
	buckets    []float64

	mu     sync.Mutex
	series map[string]*histSeries
}

type histSeries struct {
	values []string
	counts []uint64 // per bucket, not cumulative
	sum    float64
	count  uint64
}

// NewHistogram registers a histogram family. Buckets are upper bounds in
// increasing order; +Inf is added automatically.
func (r *Registry) NewHistogram(name, help string, buckets []float64, labels ...string) *Histogram {
	b := slices.Clone(buckets)
	slices.Sort(b)
	h := &Histogram{name: name, help: help, labels: labels, buckets: b, series: map[string]*histSeries{}}
	r.add(h)
	return h
}

// Observe records one value.
func (h *Histogram) Observe(v float64, values ...string) {
	values = fit(values, len(h.labels))
	k := key(values)
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &histSeries{values: values, counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, ub := range h.buckets {
		if v <= ub {
			s.counts[i]++
			break
		}
	}
	s.sum += v
	s.count++
}

func (h *Histogram) write(w *bufio.Writer) {
	header(w, h.name, h.help, "histogram")
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.series) {
		s := h.series[k]
		var cum uint64
		for i, ub := range h.buckets {
			cum += s.counts[i]
			sample(w, h.name+"_bucket", h.labels, s.values, "le", formatFloat(ub), float64(cum))
		}
		sample(w, h.name+"_bucket", h.labels, s.values, "le", "+Inf", float64(s.count))
		sample(w, h.name+"_sum", h.labels, s.values, "", "", s.sum)
		sample(w, h.name+"_count", h.labels, s.values, "", "", float64(s.count))
	}
}

// Sample is one gauge value read at scrape time.
type Sample struct {
	Values []string
	Value  float64
}

// gaugeFunc is a gauge family whose values are read when scraped.
type gaugeFunc struct {
	name, help string
	labels     []string
	collect    func() []Sample
}

// NewGaugeFunc registers a gauge family read by collect at each scrape.
// collect may return nothing, for example when its source is unavailable.
func (r *Registry) NewGaugeFunc(name, help string, labels []string, collect func() []Sample) {
	r.add(&gaugeFunc{name: name, help: help, labels: labels, collect: collect})
}

func (g *gaugeFunc) write(w *bufio.Writer) {
	header(w, g.name, g.help, "gauge")
	samples := g.collect()
	slices.SortFunc(samples, func(a, b Sample) int {
		return strings.Compare(key(a.Values), key(b.Values))
	})
	for _, s := range samples {
		sample(w, g.name, g.labels, fit(s.Values, len(g.labels)), "", "", s.Value)
	}
}

func header(w *bufio.Writer, name, help, kind string) {
	for _, part := range []string{"# HELP ", name, " ", escapeHelp(help), "\n# TYPE ", name, " ", kind, "\n"} {
		w.WriteString(part)
	}
}

// sample writes one line. extraName and extraValue add a final label, such
// as a histogram bucket's le.
func sample(w *bufio.Writer, name string, labels, values []string, extraName, extraValue string, v float64) {
	w.WriteString(name)
	if len(labels) > 0 || extraName != "" {
		w.WriteByte('{')
		first := true
		for i, l := range labels {
			if !first {
				w.WriteByte(',')
			}
			first = false
			label(w, l, escapeLabel(values[i]))
		}
		if extraName != "" {
			if !first {
				w.WriteByte(',')
			}
			label(w, extraName, extraValue)
		}
		w.WriteByte('}')
	}
	w.WriteByte(' ')
	w.WriteString(formatFloat(v))
	w.WriteByte('\n')
}

func label(w *bufio.Writer, name, value string) {
	w.WriteString(name)
	w.WriteString(`="`)
	w.WriteString(value)
	w.WriteByte('"')
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

var (
	labelEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
)

func escapeLabel(s string) string { return labelEscaper.Replace(s) }
func escapeHelp(s string) string  { return helpEscaper.Replace(s) }

// fit pads or trims label values to n.
func fit(values []string, n int) []string {
	if len(values) == n {
		return values
	}
	out := make([]string, n)
	copy(out, values)
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

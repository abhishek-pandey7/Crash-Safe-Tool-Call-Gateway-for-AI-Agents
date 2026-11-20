package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b strings.Builder
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestCounterOutput(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("calls_total", "Calls handled.", "tool", "outcome")
	c.Inc("send_email", "completed")
	c.Inc("create_refund", "unknown")
	c.Add(2, "create_refund", "unknown")
	c.Add(-5, "create_refund", "unknown") // ignored

	want := `# HELP calls_total Calls handled.
# TYPE calls_total counter
calls_total{tool="create_refund",outcome="unknown"} 3
calls_total{tool="send_email",outcome="completed"} 1
`
	if got := render(t, r); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	if v := c.Value("create_refund", "unknown"); v != 3 {
		t.Fatalf("value %v", v)
	}
}

func TestHistogramOutput(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("latency_seconds", "Latency.", []float64{1, 0.1}, "policy")
	h.Observe(0.05, "at_most_once")
	h.Observe(0.5, "at_most_once")
	h.Observe(3, "at_most_once")

	want := `# HELP latency_seconds Latency.
# TYPE latency_seconds histogram
latency_seconds_bucket{policy="at_most_once",le="0.1"} 1
latency_seconds_bucket{policy="at_most_once",le="1"} 2
latency_seconds_bucket{policy="at_most_once",le="+Inf"} 3
latency_seconds_sum{policy="at_most_once"} 3.55
latency_seconds_count{policy="at_most_once"} 3
`
	if got := render(t, r); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestGaugeFuncAndEscaping(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeFunc("operations", "Open operations by state.\nSecond line.", []string{"state"}, func() []Sample {
		return []Sample{{Values: []string{"unknown"}, Value: 4}, {Values: []string{`a"b\c`}, Value: 1}}
	})
	r.NewCounter("unlabelled_total", "No labels.").Inc()

	want := `# HELP operations Open operations by state.\nSecond line.
# TYPE operations gauge
operations{state="a\"b\\c"} 1
operations{state="unknown"} 4
# HELP unlabelled_total No labels.
# TYPE unlabelled_total counter
unlabelled_total 1
`
	if got := render(t, r); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestHandlerContentType(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("x_total", "x").Inc()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/-/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("content type %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "x_total 1") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestEmptyGaugeWritesOnlyHeader(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeFunc("g", "help", []string{"s"}, func() []Sample { return nil })
	if got := render(t, r); got != "# HELP g help\n# TYPE g gauge\n" {
		t.Fatalf("got %q", got)
	}
}

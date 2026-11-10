package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store/storetest"
)

var (
	buildOnce sync.Once
	buildDir  string
	binPath   string
	buildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

// gatewayBinary builds the real gateway once, with kill points compiled in.
func gatewayBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "crashsafe-gateway-")
		if buildErr != nil {
			return
		}
		name := "gateway"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		binPath = filepath.Join(buildDir, name)
		out, err := exec.Command("go", "build", "-tags", "killpoints", "-o", binPath, "../cmd/gateway").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build gateway: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

// syncBuffer collects a process's output.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// proc is a gateway running as its own operating system process.
type proc struct {
	name   string
	cmd    *exec.Cmd
	base   string
	url    string
	logs   *syncBuffer
	exited chan struct{}
}

type procOptions struct {
	instance string
	killAt   string
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// processDB returns a fresh schema URL with a small connection pool, so
// many gateway processes fit within Postgres's connection limit.
func processDB(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(storetest.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("pool_max_conns", "4")
	u.RawQuery = q.Encode()
	return u.String()
}

// writeConfig writes the end-to-end policy set for a given upstream.
func writeConfig(t *testing.T, upstream, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, fmt.Appendf(nil, baseConfig, upstream, extra), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// startGateway runs a gateway process and waits until it is ready.
func startGateway(t *testing.T, cfgPath, dbURL string, o procOptions) *proc {
	t.Helper()
	port := strconv.Itoa(freePort(t))
	p := &proc{
		name:   o.instance,
		base:   "http://127.0.0.1:" + port,
		logs:   &syncBuffer{},
		exited: make(chan struct{}),
	}
	p.url = p.base + "/mcp"
	p.cmd = exec.Command(gatewayBinary(t), "-config", cfgPath)
	p.cmd.Env = append(os.Environ(),
		"GATEWAY_DATABASE_URL="+dbURL,
		"GATEWAY_LISTEN=127.0.0.1:"+port,
		"GATEWAY_INSTANCE_ID="+o.instance,
		"GATEWAY_KILL_AT="+o.killAt,
	)
	p.cmd.Stdout, p.cmd.Stderr = p.logs, p.logs
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		p.kill()
		if t.Failed() {
			t.Logf("--- gateway %s output ---\n%s", p.name, p.logs.String())
		}
	})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.exited:
			t.Fatalf("gateway %s exited during startup:\n%s", p.name, p.logs.String())
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/-/readyz", nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("gateway %s not ready:\n%s", p.name, p.logs.String())
	return nil
}

// kill stops the process abruptly, like SIGKILL.
func (p *proc) kill() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.exited
}

// waitExit reports whether the process ended within d.
func (p *proc) waitExit(d time.Duration) bool {
	select {
	case <-p.exited:
		return true
	case <-time.After(d):
		return false
	}
}

func (p *proc) running() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

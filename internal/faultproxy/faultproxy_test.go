package faultproxy

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"
)

// echoServer echoes each line it receives.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

func start(t *testing.T) *Proxy {
	t.Helper()
	p, err := Start(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func dial(t *testing.T, p *Proxy) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, bufio.NewReader(c)
}

// roundTrip sends a line and reads the echo, or returns the read error.
func roundTrip(c net.Conn, r *bufio.Reader, msg string, wait time.Duration) (string, error) {
	_ = c.SetDeadline(time.Now().Add(wait))
	if _, err := io.WriteString(c, msg+"\n"); err != nil {
		return "", err
	}
	line, err := r.ReadString('\n')
	return line, err
}

func TestPassForwards(t *testing.T) {
	p := start(t)
	c, r := dial(t, p)
	got, err := roundTrip(c, r, "hello", 2*time.Second)
	if err != nil || got != "hello\n" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestDownClosesOpenAndNewConnections(t *testing.T) {
	p := start(t)
	c, r := dial(t, p)
	if _, err := roundTrip(c, r, "before", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	p.Set(Down)
	if _, err := roundTrip(c, r, "during", 2*time.Second); err == nil {
		t.Fatal("open connection still works while down")
	}
	c2, r2 := dial(t, p)
	if _, err := roundTrip(c2, r2, "new", 2*time.Second); err == nil {
		t.Fatal("new connection works while down")
	}

	p.Set(Pass)
	c3, r3 := dial(t, p)
	if got, err := roundTrip(c3, r3, "after", 2*time.Second); err != nil || got != "after\n" {
		t.Fatalf("after recovery: %q, %v", got, err)
	}
}

func TestHangHoldsDataUntilPass(t *testing.T) {
	p := start(t)
	c, r := dial(t, p)
	if _, err := roundTrip(c, r, "before", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	p.Set(Hang)
	_, err := roundTrip(c, r, "held", 300*time.Millisecond)
	var ne net.Error
	if err == nil || !asNetErr(err, &ne) || !ne.Timeout() {
		t.Fatalf("expected a timeout while hung, got %v", err)
	}

	p.Set(Pass)
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	if err != nil || line != "held\n" {
		t.Fatalf("held data after recovery: %q, %v", line, err)
	}
}

func TestHangThenDownDropsHeldData(t *testing.T) {
	p := start(t)
	c, r := dial(t, p)
	p.Set(Hang)
	_, _ = roundTrip(c, r, "held", 200*time.Millisecond)
	p.Set(Down)
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if line, err := r.ReadString('\n'); err == nil {
		t.Fatalf("held data delivered after going down: %q", line)
	}
}

func TestNewConnectionWaitsOutAHang(t *testing.T) {
	p := start(t)
	p.Set(Hang)
	c, r := dial(t, p)
	if _, err := roundTrip(c, r, "x", 300*time.Millisecond); err == nil {
		t.Fatal("new connection answered while hung")
	}
	p.Set(Pass)
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if line, err := r.ReadString('\n'); err != nil || line != "x\n" {
		t.Fatalf("after hang: %q, %v", line, err)
	}
}

func asNetErr(err error, target *net.Error) bool {
	ne, ok := err.(net.Error)
	if ok {
		*target = ne
	}
	return ok
}

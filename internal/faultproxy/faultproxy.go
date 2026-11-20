// Package faultproxy is a TCP proxy for tests. It sits in front of a
// dependency, such as Postgres, and can cut or stall the connection on
// demand, so tests can see what the gateway does during an outage.
package faultproxy

import (
	"net"
	"sync"
)

// Mode is how the proxy treats traffic.
type Mode int

const (
	// Pass forwards traffic both ways.
	Pass Mode = iota
	// Down closes every open connection and every new one straight away,
	// like a dependency that has crashed.
	Down
	// Hang accepts connections but forwards nothing until the mode changes,
	// like a network partition. Data held during a hang is delivered if the
	// mode returns to Pass and dropped if it moves to Down.
	Hang
)

func (m Mode) String() string {
	switch m {
	case Pass:
		return "pass"
	case Down:
		return "down"
	case Hang:
		return "hang"
	}
	return "unknown"
}

// Proxy forwards connections from its listener to a target address.
type Proxy struct {
	target string
	ln     net.Listener

	mu     sync.Mutex
	cond   *sync.Cond
	mode   Mode
	closed bool
	pairs  map[*pair]struct{}
	wg     sync.WaitGroup
}

// pair is one client connection and its upstream connection.
type pair struct {
	client, upstream net.Conn
	done             bool
}

// Start listens on a free local port and forwards to target.
func Start(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{target: target, ln: ln, pairs: map[*pair]struct{}{}}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address clients connect to.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Mode returns the current mode.
func (p *Proxy) Mode() Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

// Set changes the mode. Moving to Down closes every open connection.
func (p *Proxy) Set(m Mode) {
	p.mu.Lock()
	p.mode = m
	var drop []*pair
	if m == Down {
		for pr := range p.pairs {
			drop = append(drop, pr)
		}
	}
	p.cond.Broadcast()
	p.mu.Unlock()
	for _, pr := range drop {
		p.closePair(pr)
	}
}

// Close stops the proxy and closes every connection.
func (p *Proxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	var drop []*pair
	for pr := range p.pairs {
		drop = append(drop, pr)
	}
	p.cond.Broadcast()
	p.mu.Unlock()
	err := p.ln.Close()
	for _, pr := range drop {
		p.closePair(pr)
	}
	p.wg.Wait()
	return err
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.serve(c)
	}
}

// waitPass blocks while the proxy hangs. It reports whether traffic for pr
// may flow: false once the proxy is down or closed, or pr has ended.
func (p *Proxy) waitPass(pr *pair) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.mode == Hang && !p.closed && (pr == nil || !pr.done) {
		p.cond.Wait()
	}
	return p.mode == Pass && !p.closed && (pr == nil || !pr.done)
}

func (p *Proxy) serve(client net.Conn) {
	defer p.wg.Done()
	if !p.waitPass(nil) {
		client.Close()
		return
	}
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	pr := &pair{client: client, upstream: upstream}
	p.mu.Lock()
	if p.closed || p.mode == Down {
		p.mu.Unlock()
		client.Close()
		upstream.Close()
		return
	}
	p.pairs[pr] = struct{}{}
	p.mu.Unlock()

	var inner sync.WaitGroup
	inner.Add(2)
	go func() { defer inner.Done(); p.pipe(pr, upstream, client) }()
	go func() { defer inner.Done(); p.pipe(pr, client, upstream) }()
	inner.Wait()
}

// pipe copies src to dst, holding data while the proxy hangs.
func (p *Proxy) pipe(pr *pair, dst, src net.Conn) {
	defer p.closePair(pr)
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.waitPass(pr) {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) closePair(pr *pair) {
	p.mu.Lock()
	if pr.done {
		p.mu.Unlock()
		return
	}
	pr.done = true
	delete(p.pairs, pr)
	p.cond.Broadcast()
	p.mu.Unlock()
	pr.client.Close()
	pr.upstream.Close()
}

package mcp

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// Event is one Server-Sent Events event.
type Event struct {
	Name string
	Data []byte
}

// SSEReader reads events from a text/event-stream body. Comment lines and
// the id and retry fields are ignored, since the 2026-07-28 revision has no
// stream resumption.
type SSEReader struct {
	r *bufio.Reader
}

// NewSSEReader wraps r.
func NewSSEReader(r io.Reader) *SSEReader {
	return &SSEReader{r: bufio.NewReaderSize(r, 64*1024)}
}

// Next returns the next event that carries data. It returns io.EOF at a
// clean end of stream and io.ErrUnexpectedEOF when the stream ends inside an
// event.
func (s *SSEReader) Next() (Event, error) {
	var (
		ev      Event
		data    bytes.Buffer
		hasData bool
		started bool
	)
	for {
		line, err := s.r.ReadString('\n')
		if err != nil && (err != io.EOF || line == "") {
			if err == io.EOF && started {
				return Event{}, io.ErrUnexpectedEOF
			}
			return Event{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if hasData {
				ev.Data = data.Bytes()
				return ev, nil
			}
			ev, started = Event{}, false
			continue
		}
		started = true
		if line[0] == ':' {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.Name = value
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			hasData = true
		}
		if err == io.EOF {
			// A final line without a trailing newline ends the stream
			// inside an event.
			return Event{}, io.ErrUnexpectedEOF
		}
	}
}

// WriteSSE writes data as a single "message" event.
func WriteSSE(w io.Writer, data []byte) error {
	var b bytes.Buffer
	b.WriteString("event: message\n")
	for line := range bytes.SplitSeq(data, []byte("\n")) {
		b.WriteString("data: ")
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	_, err := w.Write(b.Bytes())
	return err
}

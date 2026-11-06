// Package canonjson produces a canonical byte form of a JSON value, so that
// two encodings of the same arguments hash to the same operation identity.
//
// It follows RFC 8785 (JSON Canonicalization Scheme) for structure: no
// insignificant whitespace, object members sorted by their UTF-16 code
// units, and minimal string escaping.
//
// It deliberately differs from RFC 8785 for numbers. RFC 8785 serialises a
// number through an IEEE-754 double, so distinct integers above 2^53 collapse
// to the same output and two different operations would share a key. Here
// a number literal is reduced exactly to its significant digits and a
// decimal exponent instead: 1.50, 1.5 and 15e-1 all become "15e-1", and
// 9007199254740993 stays distinct from 9007199254740992. The output is only
// ever hashed, never sent anywhere, so this costs no interoperability.
//
// Duplicate object member names and invalid UTF-8 are rejected, because
// different parsers resolve them differently.
package canonjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// maxExponent bounds the decimal exponent of a number literal.
const maxExponent = 1 << 30

// Canonicalize returns the canonical form of a single JSON value.
func Canonicalize(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("canonjson: invalid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out bytes.Buffer
	if err := writeValue(&out, dec); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("canonjson: trailing data after value")
	}
	return out.Bytes(), nil
}

func writeValue(out *bytes.Buffer, dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("canonjson: %w", err)
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			return writeObject(out, dec)
		case '[':
			return writeArray(out, dec)
		}
		return fmt.Errorf("canonjson: unexpected %q", v)
	case string:
		writeString(out, v)
	case json.Number:
		n, err := normalizeNumber(string(v))
		if err != nil {
			return err
		}
		out.WriteString(n)
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case nil:
		out.WriteString("null")
	default:
		return fmt.Errorf("canonjson: unexpected token %T", tok)
	}
	return nil
}

type member struct {
	key   string
	order []uint16
	value []byte
}

func writeObject(out *bytes.Buffer, dec *json.Decoder) error {
	var members []member
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("canonjson: %w", err)
		}
		key, ok := tok.(string)
		if !ok {
			return errors.New("canonjson: object key is not a string")
		}
		if seen[key] {
			return fmt.Errorf("canonjson: duplicate member %q", key)
		}
		seen[key] = true
		var val bytes.Buffer
		if err := writeValue(&val, dec); err != nil {
			return err
		}
		members = append(members, member{key: key, order: utf16.Encode([]rune(key)), value: val.Bytes()})
	}
	if _, err := dec.Token(); err != nil { // closing brace
		return fmt.Errorf("canonjson: %w", err)
	}
	slices.SortFunc(members, func(a, b member) int { return slices.Compare(a.order, b.order) })
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		writeString(out, m.key)
		out.WriteByte(':')
		out.Write(m.value)
	}
	out.WriteByte('}')
	return nil
}

func writeArray(out *bytes.Buffer, dec *json.Decoder) error {
	out.WriteByte('[')
	for i := 0; dec.More(); i++ {
		if i > 0 {
			out.WriteByte(',')
		}
		if err := writeValue(out, dec); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil { // closing bracket
		return fmt.Errorf("canonjson: %w", err)
	}
	out.WriteByte(']')
	return nil
}

// writeString escapes as RFC 8785 does: only the quote, the backslash and
// control characters, with short escapes where JSON has them.
func writeString(out *bytes.Buffer, s string) {
	const hex = "0123456789abcdef"
	out.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if c < 0x20 {
				out.WriteString(`\u00`)
				out.WriteByte(hex[c>>4])
				out.WriteByte(hex[c&0xf])
			} else {
				out.WriteByte(c)
			}
		}
	}
	out.WriteByte('"')
}

// normalizeNumber reduces a JSON number literal to sign, significant digits
// and a decimal exponent, without any rounding.
func normalizeNumber(lit string) (string, error) {
	s := lit
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	mantissa, expPart, hasExp := strings.Cut(strings.ToLower(s), "e")
	intPart, fracPart, _ := strings.Cut(mantissa, ".")
	digits := intPart + fracPart
	exp := -len(fracPart)
	if hasExp {
		e, err := strconv.Atoi(expPart)
		if err != nil || e > maxExponent || e < -maxExponent {
			return "", fmt.Errorf("canonjson: number %q out of range", lit)
		}
		exp += e
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return "", fmt.Errorf("canonjson: invalid number %q", lit)
		}
	}

	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0", nil // also folds -0 into 0
	}
	trimmed := strings.TrimRight(digits, "0")
	exp += len(digits) - len(trimmed)

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(trimmed)
	if exp != 0 {
		b.WriteByte('e')
		b.WriteString(strconv.Itoa(exp))
	}
	return b.String(), nil
}

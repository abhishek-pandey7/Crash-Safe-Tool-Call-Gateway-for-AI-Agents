package canonjson

import "testing"

func canon(t *testing.T, in string) string {
	t.Helper()
	out, err := Canonicalize([]byte(in))
	if err != nil {
		t.Fatalf("canonicalize %s: %v", in, err)
	}
	return string(out)
}

func TestSameValueSameBytes(t *testing.T) {
	groups := [][]string{
		{`{"b":1,"a":[true,null,"x"]}`, ` { "a" : [ true , null , "x" ] , "b" : 1 } `},
		{`1.5`, `1.50`, `15e-1`, `0.15E1`, `150e-2`},
		{`100`, `1e2`, `1E+2`, `100.0`},
		{`0`, `-0`, `0.000`, `0e10`},
		{`"A"`, `"A"`},
		{`"\/"`, `"/"`},
	}
	for _, g := range groups {
		want := canon(t, g[0])
		for _, in := range g[1:] {
			if got := canon(t, in); got != want {
				t.Errorf("%s -> %s, but %s -> %s", g[0], want, in, got)
			}
		}
	}
}

func TestLargeIntegersStayDistinct(t *testing.T) {
	// RFC 8785 maps both of these to 9007199254740992.
	a := canon(t, `{"amount":9007199254740992}`)
	b := canon(t, `{"amount":9007199254740993}`)
	if a == b {
		t.Fatalf("distinct integers collided: %s", a)
	}
}

func TestDifferentValuesDiffer(t *testing.T) {
	pairs := [][2]string{
		{`1`, `"1"`},
		{`1.5`, `-1.5`},
		{`[1,2]`, `[2,1]`},
		{`{"a":1}`, `{"a":1,"b":null}`},
		{`0.1`, `0.01`},
	}
	for _, p := range pairs {
		if canon(t, p[0]) == canon(t, p[1]) {
			t.Errorf("%s and %s collided", p[0], p[1])
		}
	}
}

func TestKeyOrderUsesUTF16(t *testing.T) {
	// U+FFFD sorts after U+1F600 by code point, but before it in UTF-16
	// because the emoji's leading surrogate is 0xD83D.
	got := canon(t, `{"�":1,"😀":2,"a":3}`)
	want := `{"a":3,"😀":2,"` + "�" + `":1}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestStringEscaping(t *testing.T) {
	got := canon(t, `"a\"b\\c\n\u0001<>& "`)
	want := `"a\"b\\c\n\u0001<>&` + " " + `"`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRejects(t *testing.T) {
	for _, in := range []string{
		`{"a":1,"a":2}`,
		`{"a":1} {"b":2}`,
		"\"\xff\"",
		`1e99999999999`,
		`{"a":`,
	} {
		if _, err := Canonicalize([]byte(in)); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
}

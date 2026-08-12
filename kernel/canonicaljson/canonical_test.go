package canonicaljson

import (
	"strings"
	"testing"
)

func TestCanonicalizeSortsKeysAndNormalizesNumbers(t *testing.T) {
	canonical, err := Canonicalize([]byte(`{"z":1.2300,"a":[1e0,-0.0,0.00100]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":[1,0,0.001],"z":1.23}`
	if string(canonical) != want {
		t.Fatalf("canonical = %s, want %s", canonical, want)
	}
}

func TestDigestIsIndependentOfMemberOrderAndNumberSpelling(t *testing.T) {
	left, err := Digest([]byte(`{"b":1.0,"a":true}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := Digest([]byte(` { "a": true, "b": 1e0 } `))
	if err != nil {
		t.Fatal(err)
	}
	if left != right || !strings.HasPrefix(left, "sha256:") || len(left) != 71 {
		t.Fatalf("digests = %q and %q", left, right)
	}
}

func TestDecodeRejectsDuplicateAndTrailingValues(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `true false`} {
		if _, err := Canonicalize([]byte(raw)); err == nil {
			t.Fatalf("expected rejection for %s", raw)
		}
	}
}

func TestDecodeEnforcesDepthAndExponentLimits(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxDepth = 2
	if _, err := CanonicalizeWithLimits([]byte(`[[null]]`), limits); err == nil {
		t.Fatal("expected depth rejection")
	}
	limits = DefaultLimits()
	limits.MaxExponent = 4
	if _, err := CanonicalizeWithLimits([]byte(`1e5`), limits); err == nil {
		t.Fatal("expected exponent rejection")
	}
}

func TestCanonicalStringsDoNotUseHostHTMLEscaping(t *testing.T) {
	canonical, err := Canonicalize([]byte(`{"value":"<>&\u2028"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != "{\"value\":\"<>&\u2028\"}" {
		t.Fatalf("canonical string = %q", canonical)
	}
	if _, err := Canonicalize([]byte{'"', 0xff, '"'}); err == nil {
		t.Fatal("expected invalid UTF-8 rejection")
	}
}

func TestCanonicalStringsRejectUnpairedSurrogates(t *testing.T) {
	for _, raw := range []string{`"\ud800"`, `"\udfff"`} {
		if _, err := Canonicalize([]byte(raw)); err == nil || !strings.Contains(err.Error(), "unpaired") {
			t.Fatalf("surrogate error for %s = %v", raw, err)
		}
	}
	canonical, err := Canonicalize([]byte(`"\ud83d\ude80"`))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `"🚀"` {
		t.Fatalf("surrogate pair = %s", canonical)
	}
}

func TestUnmarshalStrictRejectsUnknownFields(t *testing.T) {
	var target struct {
		Name string `json:"name"`
	}
	if err := UnmarshalStrict([]byte(`{"name":"ok","legacy":true}`), &target); err == nil {
		t.Fatal("expected unknown field rejection")
	}
}

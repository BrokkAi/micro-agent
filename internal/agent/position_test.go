package agent

import (
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

func samePosition(a, b schema.Position) bool {
	return a.Line == b.Line && a.Character == b.Character
}

func TestChoosePositionEncoding(t *testing.T) {
	for name, test := range map[string]struct {
		offered []schema.PositionEncodingKind
		want    schema.PositionEncodingKind
	}{
		"empty":     {nil, schema.PositionEncodingKindUtf16},
		"prefers 8": {[]schema.PositionEncodingKind{schema.PositionEncodingKindUtf16, schema.PositionEncodingKindUtf8}, schema.PositionEncodingKindUtf8},
		"16 only":   {[]schema.PositionEncodingKind{schema.PositionEncodingKindUtf16}, schema.PositionEncodingKindUtf16},
		"unknown":   {[]schema.PositionEncodingKind{"utf-7"}, schema.PositionEncodingKindUtf16},
		"32 and 16": {[]schema.PositionEncodingKind{schema.PositionEncodingKindUtf32, schema.PositionEncodingKindUtf16}, schema.PositionEncodingKindUtf16},
	} {
		if got := choosePositionEncoding(test.offered); got != test.want {
			t.Errorf("%s: got %s, want %s", name, got, test.want)
		}
	}
}

func TestPositionConversions(t *testing.T) {
	text := "héllo\nwörld\nemoji: 😀!"
	cases := []struct {
		encoding schema.PositionEncodingKind
		position schema.Position
		offset   int
	}{
		{schema.PositionEncodingKindUtf8, schema.Position{Line: 0, Character: 1}, 1},
		{schema.PositionEncodingKindUtf8, schema.Position{Line: 1, Character: 1}, 8},
		{schema.PositionEncodingKindUtf16, schema.Position{Line: 1, Character: 1}, 8},
		{schema.PositionEncodingKindUtf16, schema.Position{Line: 1, Character: 2}, 10},
		{schema.PositionEncodingKindUtf16, schema.Position{Line: 2, Character: 7}, 14 + 7},
		{schema.PositionEncodingKindUtf16, schema.Position{Line: 2, Character: 9}, 14 + 7 + 4},
		{schema.PositionEncodingKindUtf16, schema.Position{Line: 9, Character: 0}, len(text)},
		{schema.PositionEncodingKindUtf16, schema.Position{Line: 0, Character: 99}, 6},
	}
	for _, c := range cases {
		if got := byteOffset(text, c.position, c.encoding); got != c.offset {
			t.Errorf("byteOffset(%s, %v) = %d, want %d", c.encoding, c.position, got, c.offset)
		}
		// Clamped and out-of-range positions do not round-trip.
		if c.position.Character == 99 || c.position.Line > 2 {
			continue
		}
		if got := positionAt(text, c.offset, c.encoding); !samePosition(got, c.position) {
			t.Errorf("positionAt(%s, %d) = %v, want %v", c.encoding, c.offset, got, c.position)
		}
	}
	if got := positionAt("a😀b", 5, schema.PositionEncodingKindUtf16); !samePosition(got, schema.Position{Line: 0, Character: 3}) {
		t.Errorf("emoji position = %v", got)
	}
	if got := byteOffset("a😀b", schema.Position{Line: 0, Character: 3}, schema.PositionEncodingKindUtf16); got != 5 {
		t.Errorf("emoji offset = %d", got)
	}
}

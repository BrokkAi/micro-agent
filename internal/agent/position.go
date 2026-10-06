package agent

import (
	"strings"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

// choosePositionEncoding picks the client's most preferred encoding that
// micro-agent speaks; without a preference the protocol default, UTF-16.
func choosePositionEncoding(supported []schema.PositionEncodingKind) schema.PositionEncodingKind {
	for _, kind := range []schema.PositionEncodingKind{schema.PositionEncodingKindUtf8, schema.PositionEncodingKindUtf16} {
		for _, offered := range supported {
			if offered == kind {
				return kind
			}
		}
	}
	return schema.PositionEncodingKindUtf16
}

// byteOffset converts a document position in the negotiated encoding to a
// byte offset into text. Positions past a line's end clamp to it.
func byteOffset(text string, pos schema.Position, encoding schema.PositionEncodingKind) int {
	offset := 0
	for line := uint32(0); line < pos.Line; line++ {
		newline := strings.IndexByte(text[offset:], '\n')
		if newline < 0 {
			return len(text)
		}
		offset += newline + 1
	}
	line := text[offset:]
	if newline := strings.IndexByte(line, '\n'); newline >= 0 {
		line = line[:newline]
	}
	return offset + offsetInLine(line, pos.Character, encoding)
}

// offsetInLine converts a character index in the negotiated encoding to a
// byte offset within one line.
func offsetInLine(line string, character uint32, encoding schema.PositionEncodingKind) int {
	if encoding == schema.PositionEncodingKindUtf8 {
		if int(character) > len(line) {
			return len(line)
		}
		return int(character)
	}
	var units uint32
	for i, r := range line {
		if units >= character {
			return i
		}
		if encoding == schema.PositionEncodingKindUtf16 && r > 0xFFFF {
			units += 2
		} else {
			units++
		}
	}
	return len(line)
}

// positionAt converts a byte offset into text to a position in the
// negotiated encoding.
func positionAt(text string, offset int, encoding schema.PositionEncodingKind) schema.Position {
	if offset > len(text) {
		offset = len(text)
	}
	if offset < 0 {
		offset = 0
	}
	var line uint32
	start := 0
	for i := 0; i < offset; i++ {
		if text[i] == '\n' {
			line++
			start = i + 1
		}
	}
	units := uint32(offset - start)
	if encoding != schema.PositionEncodingKindUtf8 {
		units = 0
		for _, r := range text[start:offset] {
			if encoding == schema.PositionEncodingKindUtf16 && r > 0xFFFF {
				units += 2
			} else {
				units++
			}
		}
	}
	return schema.Position{Line: line, Character: units}
}

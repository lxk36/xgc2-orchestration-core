// Package canonicaljson defines the bounded, deterministic JSON profile used
// for content identities in the XGC orchestration protocol.
package canonicaljson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	DefaultMaxInputBytes     = 1 << 20
	DefaultMaxCanonicalBytes = 1 << 20
	DefaultMaxDepth          = 128
	DefaultMaxNodes          = 100_000
	DefaultMaxStringBytes    = 1 << 20
	DefaultMaxExponent       = 10_000
)

type Limits struct {
	MaxInputBytes     int
	MaxCanonicalBytes int
	MaxDepth          int
	MaxNodes          int
	MaxStringBytes    int
	MaxExponent       int
}

func DefaultLimits() Limits {
	return Limits{
		MaxInputBytes:     DefaultMaxInputBytes,
		MaxCanonicalBytes: DefaultMaxCanonicalBytes,
		MaxDepth:          DefaultMaxDepth,
		MaxNodes:          DefaultMaxNodes,
		MaxStringBytes:    DefaultMaxStringBytes,
		MaxExponent:       DefaultMaxExponent,
	}
}

func (limits Limits) validate() error {
	if limits.MaxInputBytes <= 0 || limits.MaxCanonicalBytes <= 0 || limits.MaxDepth <= 0 ||
		limits.MaxNodes <= 0 || limits.MaxStringBytes <= 0 || limits.MaxExponent <= 0 {
		return errors.New("canonical JSON limits must all be positive")
	}
	return nil
}

// Decode accepts exactly one JSON value, preserves number spelling until
// canonicalization, rejects duplicate object members, and enforces limits.
func Decode(raw []byte, limits Limits) (any, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if len(raw) > limits.MaxInputBytes {
		return nil, fmt.Errorf("JSON input exceeds %d bytes", limits.MaxInputBytes)
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("JSON input is not valid UTF-8")
	}
	if err := rejectUnpairedSurrogates(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	state := decodeState{decoder: decoder, limits: limits}
	value, err := state.value(1)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, fmt.Errorf("trailing JSON data: %w", err)
	}
	return value, nil
}

// UnmarshalStrict applies the canonical wire parser before decoding a typed
// contract and rejects fields unknown to that contract version.
func UnmarshalStrict(raw []byte, target any) error {
	canonical, err := Canonicalize(raw)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}

type decodeState struct {
	decoder *json.Decoder
	limits  Limits
	nodes   int
}

func (state *decodeState) value(depth int) (any, error) {
	if depth > state.limits.MaxDepth {
		return nil, fmt.Errorf("JSON nesting exceeds depth %d", state.limits.MaxDepth)
	}
	state.nodes++
	if state.nodes > state.limits.MaxNodes {
		return nil, fmt.Errorf("JSON value exceeds %d nodes", state.limits.MaxNodes)
	}
	token, err := state.decoder.Token()
	if err != nil {
		return nil, err
	}
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			return state.object(depth)
		case '[':
			return state.array(depth)
		default:
			return nil, fmt.Errorf("unexpected delimiter %q", typed)
		}
	case string:
		if len(typed) > state.limits.MaxStringBytes {
			return nil, fmt.Errorf("JSON string exceeds %d bytes", state.limits.MaxStringBytes)
		}
		return typed, nil
	case json.Number:
		if _, err := normalizeNumber(typed.String(), state.limits); err != nil {
			return nil, err
		}
		return typed, nil
	case bool, nil:
		return typed, nil
	default:
		return nil, fmt.Errorf("unsupported JSON token %T", token)
	}
}

func (state *decodeState) object(depth int) (map[string]any, error) {
	result := make(map[string]any)
	for state.decoder.More() {
		keyToken, err := state.decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("object member name is not a string")
		}
		if len(key) > state.limits.MaxStringBytes {
			return nil, fmt.Errorf("JSON object key exceeds %d bytes", state.limits.MaxStringBytes)
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("duplicate JSON object member %q", key)
		}
		value, err := state.value(depth + 1)
		if err != nil {
			return nil, fmt.Errorf("object member %q: %w", key, err)
		}
		result[key] = value
	}
	if token, err := state.decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("unterminated JSON object")
	}
	return result, nil
}

func (state *decodeState) array(depth int) ([]any, error) {
	result := make([]any, 0)
	for state.decoder.More() {
		value, err := state.value(depth + 1)
		if err != nil {
			return nil, fmt.Errorf("array item %d: %w", len(result), err)
		}
		result = append(result, value)
	}
	if token, err := state.decoder.Token(); err != nil || token != json.Delim(']') {
		return nil, errors.New("unterminated JSON array")
	}
	return result, nil
}

func Canonicalize(raw []byte) ([]byte, error) {
	return CanonicalizeWithLimits(raw, DefaultLimits())
}

func CanonicalizeWithLimits(raw []byte, limits Limits) ([]byte, error) {
	value, err := Decode(raw, limits)
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := appendValue(&output, value, limits); err != nil {
		return nil, err
	}
	if output.Len() > limits.MaxCanonicalBytes {
		return nil, fmt.Errorf("canonical JSON exceeds %d bytes", limits.MaxCanonicalBytes)
	}
	return output.Bytes(), nil
}

// Marshal converts a Go value through encoding/json before applying the same
// strict canonical representation used for wire JSON.
func Marshal(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return Canonicalize(raw)
}

func Digest(raw []byte) (string, error) {
	canonical, err := Canonicalize(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func DigestValue(value any) (string, error) {
	canonical, err := Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func appendValue(output *bytes.Buffer, value any, limits Limits) error {
	switch typed := value.(type) {
	case nil:
		output.WriteString("null")
	case bool:
		if typed {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
	case string:
		appendString(output, typed)
	case json.Number:
		normalized, err := normalizeNumber(typed.String(), limits)
		if err != nil {
			return err
		}
		output.WriteString(normalized)
	case []any:
		output.WriteByte('[')
		for index, child := range typed {
			if index != 0 {
				output.WriteByte(',')
			}
			if err := appendValue(output, child, limits); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index != 0 {
				output.WriteByte(',')
			}
			appendString(output, key)
			output.WriteByte(':')
			if err := appendValue(output, typed[key], limits); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("unsupported canonical JSON value %T", value)
	}
	if output.Len() > limits.MaxCanonicalBytes {
		return fmt.Errorf("canonical JSON exceeds %d bytes", limits.MaxCanonicalBytes)
	}
	return nil
}

func appendString(output *bytes.Buffer, value string) {
	const hexadecimal = "0123456789abcdef"
	output.WriteByte('"')
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch character {
		case '"', '\\':
			output.WriteByte('\\')
			output.WriteByte(character)
		case '\b':
			output.WriteString(`\b`)
		case '\f':
			output.WriteString(`\f`)
		case '\n':
			output.WriteString(`\n`)
		case '\r':
			output.WriteString(`\r`)
		case '\t':
			output.WriteString(`\t`)
		default:
			if character < 0x20 {
				output.WriteString(`\u00`)
				output.WriteByte(hexadecimal[character>>4])
				output.WriteByte(hexadecimal[character&0x0f])
			} else {
				output.WriteByte(character)
			}
		}
	}
	output.WriteByte('"')
}

func normalizeNumber(raw string, limits Limits) (string, error) {
	if raw == "" {
		return "", errors.New("empty JSON number")
	}
	negative := raw[0] == '-'
	if negative {
		raw = raw[1:]
	}
	exponent := 0
	if marker := strings.IndexAny(raw, "eE"); marker >= 0 {
		parsed, err := strconv.Atoi(raw[marker+1:])
		if err != nil {
			return "", errors.New("invalid JSON number exponent")
		}
		exponent = parsed
		raw = raw[:marker]
	}
	if exponent > limits.MaxExponent || exponent < -limits.MaxExponent {
		return "", fmt.Errorf("JSON number exponent exceeds %d", limits.MaxExponent)
	}
	integer, fraction := raw, ""
	if point := strings.IndexByte(raw, '.'); point >= 0 {
		integer, fraction = raw[:point], raw[point+1:]
	}
	if integer == "" || (len(integer) > 1 && integer[0] == '0') || !decimalDigits(integer) ||
		(fraction != "" && !decimalDigits(fraction)) {
		return "", errors.New("invalid JSON number")
	}
	digits := strings.TrimLeft(integer+fraction, "0")
	if digits == "" {
		return "0", nil
	}
	scale := len(fraction) - exponent
	var normalized string
	switch decimalPoint := len(digits) - scale; {
	case scale <= 0:
		if -scale > limits.MaxCanonicalBytes-len(digits) {
			return "", errors.New("canonical JSON number is too large")
		}
		normalized = digits + strings.Repeat("0", -scale)
	case decimalPoint > 0:
		normalized = digits[:decimalPoint] + "." + digits[decimalPoint:]
	default:
		if -decimalPoint > limits.MaxCanonicalBytes-len(digits)-2 {
			return "", errors.New("canonical JSON number is too large")
		}
		normalized = "0." + strings.Repeat("0", -decimalPoint) + digits
	}
	if point := strings.IndexByte(normalized, '.'); point >= 0 {
		normalized = strings.TrimRight(normalized, "0")
		normalized = strings.TrimSuffix(normalized, ".")
	}
	if negative {
		normalized = "-" + normalized
	}
	if len(normalized) > limits.MaxCanonicalBytes {
		return "", errors.New("canonical JSON number is too large")
	}
	return normalized, nil
}

func decimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func rejectUnpairedSurrogates(raw []byte) error {
	insideString := false
	for index := 0; index < len(raw); index++ {
		switch raw[index] {
		case '"':
			insideString = !insideString
		case '\\':
			if !insideString || index+1 >= len(raw) {
				continue
			}
			if raw[index+1] != 'u' {
				index++
				continue
			}
			code, ok := unicodeEscape(raw, index)
			if !ok {
				continue // encoding/json reports malformed hexadecimal escapes.
			}
			if code >= 0xdc00 && code <= 0xdfff {
				return errors.New("JSON string contains an unpaired low surrogate")
			}
			if code >= 0xd800 && code <= 0xdbff {
				next := index + 6
				low, paired := unicodeEscape(raw, next)
				if !paired || low < 0xdc00 || low > 0xdfff {
					return errors.New("JSON string contains an unpaired high surrogate")
				}
				index = next + 5
				continue
			}
			index += 5
		}
	}
	return nil
}

func unicodeEscape(raw []byte, start int) (uint16, bool) {
	if start < 0 || start+5 >= len(raw) || raw[start] != '\\' || raw[start+1] != 'u' {
		return 0, false
	}
	var result uint16
	for _, character := range raw[start+2 : start+6] {
		result <<= 4
		switch {
		case character >= '0' && character <= '9':
			result += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			result += uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			result += uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return result, true
}

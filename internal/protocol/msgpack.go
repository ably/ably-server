package protocol

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// errInvalidMsgpack is the error returned from MsgpackToJSON when an error is
// encountered reading from the msgpack decoder.
var errInvalidMsgpack = errors.New("invalid msgpack encoded value")

var (
	jsonNull  = []byte("null")
	jsonFalse = []byte("false")
	jsonTrue  = []byte("true")
)

// MsgpackToJSON returns the JSON encoding of the next msgpack value from the
// given decoder using as few allocations as possible.
func MsgpackToJSON(dec *msgpack.Decoder) (string, error) {
	m := msgpackToJSONPool.Get().(*msgpackToJSON)
	defer msgpackToJSONPool.Put(m)
	m.reset(dec)
	return m.convert()
}

// msgpackToJSONPool is a sync.Pool of msgpackToJSON objects which each have
// 4KiB buffers for both reading from a msgpack decoder and constructing an
// equivalent JSON encoding.
var msgpackToJSONPool = sync.Pool{
	New: func() any {
		return &msgpackToJSON{
			buf:  make([]byte, 4096),
			json: make([]byte, 0, 4096),
		}
	},
}

// msgpackToJSON is used to convert a msgpack encoded value to its equivalent
// JSON encoding using as few allocations as possible.
type msgpackToJSON struct {
	// dec is the msgpack decoder to read the msgpack encoded value from.
	dec *msgpack.Decoder

	// buf is used to repeatedly read bytes from the msgpack decoder.
	buf []byte

	// json is used to construct the JSON encoding by iteratively appending
	// the appropriate bytes whilst reading from the decoder.
	json []byte
}

// reset resets the msgpackToJSON object to decode the next value from the
// given decoder.
func (m *msgpackToJSON) reset(dec *msgpack.Decoder) {
	m.dec = dec
	m.json = m.json[:0]
}

// convert returns the JSON encoding of the next msgpack value from the
// msgpack decoder.
func (m *msgpackToJSON) convert() (string, error) {
	if err := m.appendValue(); err != nil {
		return "", err
	}
	return string(m.json), nil
}

// appendValue reads the next value from the msgpack decoder and appends its
// equivalent JSON encoding to m.json.
func (m *msgpackToJSON) appendValue() error {
	code, err := m.readByte()
	if err != nil {
		return err
	}

	switch {
	case code == msgpcode.Nil:
		m.append(jsonNull)

	case code == msgpcode.False:
		m.append(jsonFalse)

	case code == msgpcode.True:
		m.append(jsonTrue)

	case code <= msgpcode.PosFixedNumHigh:
		m.appendInt(int64(code))

	case code >= msgpcode.NegFixedNumLow:
		m.appendInt(int64(int8(code)))

	case code == msgpcode.Uint8:
		num, err := m.readUint8()
		if err != nil {
			return err
		}
		m.appendUint(uint64(num))

	case code == msgpcode.Uint16:
		num, err := m.readUint16()
		if err != nil {
			return err
		}
		m.appendUint(uint64(num))

	case code == msgpcode.Uint32:
		num, err := m.readUint32()
		if err != nil {
			return err
		}
		m.appendUint(uint64(num))

	case code == msgpcode.Uint64:
		num, err := m.readUint64()
		if err != nil {
			return err
		}
		m.appendUint(num)

	case code == msgpcode.Int8:
		num, err := m.readInt8()
		if err != nil {
			return err
		}
		m.appendInt(int64(num))

	case code == msgpcode.Int16:
		num, err := m.readInt16()
		if err != nil {
			return err
		}
		m.appendInt(int64(num))

	case code == msgpcode.Int32:
		num, err := m.readInt32()
		if err != nil {
			return err
		}
		m.appendInt(int64(num))

	case code == msgpcode.Int64:
		num, err := m.readInt64()
		if err != nil {
			return err
		}
		m.appendInt(num)

	case code == msgpcode.Float:
		b, err := m.readN(4)
		if err != nil {
			return err
		}
		num := math.Float32frombits(binary.BigEndian.Uint32(b))
		m.appendFloat32(num)

	case code == msgpcode.Double:
		b, err := m.readN(8)
		if err != nil {
			return err
		}
		num := math.Float64frombits(binary.BigEndian.Uint64(b))
		m.appendFloat64(num)

	case msgpcode.IsFixedString(code), code == msgpcode.Str8, code == msgpcode.Str16, code == msgpcode.Str32:
		str, err := m.readBytes(code)
		if err != nil {
			return err
		}
		m.appendByte('"')
		m.appendString(str)
		m.appendByte('"')

	case code == msgpcode.Bin8, code == msgpcode.Bin16, code == msgpcode.Bin32:
		bin, err := m.readBytes(code)
		if err != nil {
			return err
		}
		m.appendByte('"')
		m.appendBase64(bin)
		m.appendByte('"')

	case msgpcode.IsFixedArray(code), code == msgpcode.Array16, code == msgpcode.Array32:
		return m.appendArray(code)

	case msgpcode.IsFixedMap(code), code == msgpcode.Map16, code == msgpcode.Map32:
		return m.appendMap(code)

	case code == msgpcode.FixExt1, code == msgpcode.FixExt2, code == msgpcode.FixExt4, code == msgpcode.FixExt8, code == msgpcode.FixExt16, code == msgpcode.Ext8, code == msgpcode.Ext16, code == msgpcode.Ext32:
		return m.appendExt(code)

	default:
		return errInvalidMsgpack
	}
	return nil
}

// readByte returns the next byte from the msgpack decoder.
func (m *msgpackToJSON) readByte() (byte, error) {
	if err := m.dec.ReadFull(m.buf[:1]); err != nil {
		return 0, errInvalidMsgpack
	}
	return m.buf[0], nil
}

// readN reads the next N bytes from the msgpack decoder.
//
// The returned slice is only valid until the next call to any readXXX method.
func (m *msgpackToJSON) readN(n int64) ([]byte, error) {
	var out []byte
	// if m.buf doesn't have enough capacity to hold N bytes, allocate
	// a new slice instead (this is preferred rather than growing m.buf
	// since we don't want the sync.Pool to accumulate large slices over
	// time, and most values will fit into m.buf)
	if int64(cap(m.buf)) < n {
		out = make([]byte, n)
	} else {
		out = m.buf[:n]
	}
	if err := m.dec.ReadFull(out); err != nil {
		return nil, errInvalidMsgpack
	}
	return out, nil
}

func (m *msgpackToJSON) readUint8() (uint8, error) {
	b, err := m.readByte()
	if err != nil {
		return 0, err
	}
	return uint8(b), nil
}

func (m *msgpackToJSON) readUint16() (uint16, error) {
	b, err := m.readN(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (m *msgpackToJSON) readUint32() (uint32, error) {
	b, err := m.readN(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (m *msgpackToJSON) readUint64() (uint64, error) {
	b, err := m.readN(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (m *msgpackToJSON) readInt8() (int8, error) {
	b, err := m.readByte()
	if err != nil {
		return 0, err
	}
	return int8(b), nil
}

func (m *msgpackToJSON) readInt16() (int16, error) {
	b, err := m.readN(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (m *msgpackToJSON) readInt32() (int32, error) {
	b, err := m.readN(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (m *msgpackToJSON) readInt64() (int64, error) {
	b, err := m.readN(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// readBytes returns the data of the next value based on the given code.
//
// The returned slice is only valid until the next call to any readXXX method.
func (m *msgpackToJSON) readBytes(code byte) ([]byte, error) {
	// determine the amount of data to read based on the code
	var size int64
	switch {
	case msgpcode.IsFixedString(code):
		size = int64(code & msgpcode.FixedStrMask)
	case code == msgpcode.Str8, code == msgpcode.Bin8:
		n, err := m.readUint8()
		if err != nil {
			return nil, err
		}
		size = int64(n)
	case code == msgpcode.Str16, code == msgpcode.Bin16:
		n, err := m.readUint16()
		if err != nil {
			return nil, err
		}
		size = int64(n)
	case code == msgpcode.Str32, code == msgpcode.Bin32:
		n, err := m.readUint32()
		if err != nil {
			return nil, err
		}
		size = int64(n)
	case code == msgpcode.FixExt1:
		// fixext1 is type + 1 byte, so 2 bytes total
		size = 2
	case code == msgpcode.FixExt2:
		// fixext2 is type + 2 bytes, so 3 bytes total
		size = 3
	case code == msgpcode.FixExt4:
		// fixext4 is type + 4 bytes, so 5 bytes total
		size = 5
	case code == msgpcode.FixExt8:
		// fixext8 is type + 8 bytes, so 9 bytes total
		size = 9
	case code == msgpcode.FixExt16:
		// fixext16 is type + 16 bytes, so 17 bytes total
		size = 17
	case code == msgpcode.Ext8:
		n, err := m.readUint8()
		if err != nil {
			return nil, err
		}
		size = int64(n + 1)
	case code == msgpcode.Ext16:
		n, err := m.readUint16()
		if err != nil {
			return nil, err
		}
		size = int64(n + 1)
	case code == msgpcode.Ext32:
		n, err := m.readUint32()
		if err != nil {
			return nil, err
		}
		size = int64(n + 1)
	}
	return m.readN(size)
}

// append appends the given bytes to the JSON encoding, which are expected to
// be a utf-8 string containing no special characters.
func (m *msgpackToJSON) append(b []byte) {
	m.json = append(m.json, b...)
}

// appendByte appends the given byte to the JSON encoding, which is expected to
// be an ASCII character.
func (m *msgpackToJSON) appendByte(b byte) {
	m.json = append(m.json, b)
}

// appendInt appends the given int to the JSON encoding as a JSON number.
func (m *msgpackToJSON) appendInt(num int64) {
	m.json = strconv.AppendInt(m.json, num, 10)
}

// appendInt appends the given uint to the JSON encoding as a JSON number.
func (m *msgpackToJSON) appendUint(num uint64) {
	m.json = strconv.AppendUint(m.json, num, 10)
}

// appendFloat32 appends the given float to the JSON encoding as a JSON number.
func (m *msgpackToJSON) appendFloat32(num float32) {
	m.json = strconv.AppendFloat(m.json, float64(num), 'g', -1, 32)
}

// appendFloat64 appends the given float to the JSON encoding as a JSON number.
func (m *msgpackToJSON) appendFloat64(num float64) {
	m.json = strconv.AppendFloat(m.json, num, 'g', -1, 64)
}

// appendBase64 appends the base64 encoding of the given bytes to the JSON
// encoding.
func (m *msgpackToJSON) appendBase64(buf []byte) {
	m.json = base64.StdEncoding.AppendEncode(m.json, buf)
}

// appendArray appends the next value from the msgpack decoder to the JSON
// encoding as a JSON array.
func (m *msgpackToJSON) appendArray(code byte) error {
	// determine the number of values to encode into the JSON array
	var size uint32
	switch {
	case msgpcode.IsFixedArray(code):
		size = uint32(code & msgpcode.FixedArrayMask)
	case code == msgpcode.Array16:
		n, err := m.readUint16()
		if err != nil {
			return err
		}
		size = uint32(n)
	case code == msgpcode.Array32:
		n, err := m.readUint32()
		if err != nil {
			return err
		}
		size = n
	}

	// append the appropriate number of values into the JSON encoding,
	// surrounded by square brackets, each separated by a comma
	m.appendByte('[')
	for i := range size {
		if i > 0 {
			m.appendByte(',')
		}
		if err := m.appendValue(); err != nil {
			return err
		}
	}
	m.appendByte(']')

	return nil
}

// appendMap appends the next value from the msgpack decoder to the JSON
// encoding as a JSON object.
//
// appendMap skips any keys whose value is the msgpack-js encoding of
// 'undefined' (i.e. fixext1 with type 0 and a NULL byte) to remain compatible
// with how node converts msgpack to JSON by removing undefined values.
func (m *msgpackToJSON) appendMap(code byte) error {
	// determine the number of values to encode into the JSON object
	var size uint32
	switch {
	case msgpcode.IsFixedMap(code):
		size = uint32(code & msgpcode.FixedMapMask)
	case code == msgpcode.Map16:
		n, err := m.readUint16()
		if err != nil {
			return err
		}
		size = uint32(n)
	case code == msgpcode.Map32:
		n, err := m.readUint32()
		if err != nil {
			return err
		}
		size = n
	}

	// append the appropriate number of values into the JSON encoding,
	// surrounded by curly brackets, each separated by a comma
	m.appendByte('{')
	for i := range size {
		// read the key into a variable rather than appending directly
		// so we can read the following value and skip this key if its
		// value is msgpack-js undefined
		code, err := m.readByte()
		if err != nil {
			return err
		}
		key, err := m.readBytes(code)
		if err != nil {
			return err
		}

		// peek the next code to see if it's fixext1, and if it is,
		// skip if it has type 0 and a NULL byte (peek rather than
		// reading the code so we can use appendValue for all other
		// values, which expects to be able to read the code itself)
		nextCode, err := m.dec.PeekCode()
		if err != nil {
			return err
		}
		var fixext1Buf []byte
		if nextCode == msgpcode.FixExt1 {
			// read the fixext1 bytes into a new slice so we don't
			// invalidate the 'key' bytes, which haven't been
			// appended yet
			fixext1Buf = make([]byte, 3)
			if err := m.dec.ReadFull(fixext1Buf); err != nil {
				return errInvalidMsgpack
			}
			// skip if the type is 0 and the value a NULL byte
			if fixext1Buf[1] == 0 && fixext1Buf[2] == 0 {
				continue
			}
		}

		// we now know we're going to write the key + value
		if i > 0 {
			m.appendByte(',')
		}
		m.appendByte('"')
		m.append(key)
		m.appendByte('"')
		m.appendByte(':')

		// if we already read the fixext1 bytes, we can't use
		// appendValue since it will end up reading the next key,
		// so append the fixext1 bytes the same way appendValue
		// would
		if len(fixext1Buf) > 0 {
			m.appendByte('[')
			m.appendInt(int64(fixext1Buf[1]))
			m.appendByte(',')
			m.appendInt(int64(fixext1Buf[2]))
			m.appendByte(']')
			continue
		}

		if err := m.appendValue(); err != nil {
			return err
		}
	}
	m.appendByte('}')
	return nil
}

// appendExt appends the next extension value from the msgpack decoder to the
// JSON encoding as a JSON array like '[<type>, <data>]', where <type> is the
// extension's type as a JSON number, and '<data>' is the extension's data as
// a JSON number for fixext1, or a base64 encoded JSON string otherwise.
func (m *msgpackToJSON) appendExt(code byte) error {
	b, err := m.readBytes(code)
	if err != nil {
		return err
	}
	if len(b) < 2 {
		return errInvalidMsgpack
	}
	m.appendByte('[')
	m.appendInt(int64(b[0]))
	m.appendByte(',')
	if code == msgpcode.FixExt1 {
		m.appendInt(int64(b[1]))
	} else {
		m.appendByte('"')
		m.appendBase64(b[1:])
		m.appendByte('"')
	}
	m.appendByte(']')
	return nil
}

// appendString is adapted from the stdlib encoding/json package:
// https://github.com/golang/go/blob/34c8b14ca9f4096383d658fbd748322a993a2bd2/src/encoding/json/encode.go#L979-L1049
//
// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
func (m *msgpackToJSON) appendString(src []byte) {
	const hex = "0123456789abcdef"

	start := 0
	for i := 0; i < len(src); {
		if b := src[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			m.json = append(m.json, src[start:i]...)
			switch b {
			case '\\', '"':
				m.json = append(m.json, '\\', b)
			case '\b':
				m.json = append(m.json, '\\', 'b')
			case '\f':
				m.json = append(m.json, '\\', 'f')
			case '\n':
				m.json = append(m.json, '\\', 'n')
			case '\r':
				m.json = append(m.json, '\\', 'r')
			case '\t':
				m.json = append(m.json, '\\', 't')
			default:
				// This encodes bytes < 0x20 except for \b, \f, \n, \r and \t.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				m.json = append(m.json, '\\', 'u', '0', '0', hex[b>>4], hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		// TODO(https://go.dev/issue/56948): Use generic utf8 functionality.
		// For now, cast only a small portion of byte slices to a string
		// so that it can be stack allocated. This slows down []byte slightly
		// due to the extra copy, but keeps string performance roughly the same.
		n := min(len(src)-i, utf8.UTFMax)
		c, size := utf8.DecodeRuneInString(string(src[i : i+n]))
		if c == utf8.RuneError && size == 1 {
			m.json = append(m.json, src[start:i]...)
			m.json = append(m.json, `\ufffd`...)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See https://en.wikipedia.org/wiki/JSON#Safety.
		if c == '\u2028' || c == '\u2029' {
			m.json = append(m.json, src[start:i]...)
			m.json = append(m.json, '\\', 'u', '2', '0', '2', hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	m.json = append(m.json, src[start:]...)
}

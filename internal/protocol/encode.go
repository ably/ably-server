package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Format is the wire encoding of a ProtocolMessage frame.
type Format int

const (
	FormatJSON Format = iota
	FormatMsgpack
)

// FormatFromQuery returns the format selected by the `format` query
// parameter on a WebSocket upgrade request. Empty defaults to JSON.
func FormatFromQuery(v string) (Format, error) {
	switch v {
	case "", "json":
		return FormatJSON, nil
	case "msgpack":
		return FormatMsgpack, nil
	default:
		return 0, fmt.Errorf("unsupported format %q", v)
	}
}

// String returns the format name for diagnostics.
func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatMsgpack:
		return "msgpack"
	default:
		return "unknown"
	}
}

// Marshal encodes a ProtocolMessage in the given format.
func Marshal(m *ProtocolMessage, f Format) ([]byte, error) {
	switch f {
	case FormatJSON:
		return json.Marshal(m)
	case FormatMsgpack:
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		enc.UseCompactInts(true)
		if err := enc.Encode(m); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported format %d", f)
	}
}

// Unmarshal decodes data into m using the given format.
func Unmarshal(data []byte, f Format, m *ProtocolMessage) error {
	switch f {
	case FormatJSON:
		return json.Unmarshal(data, m)
	case FormatMsgpack:
		return msgpack.Unmarshal(data, m)
	default:
		return fmt.Errorf("unsupported format %d", f)
	}
}

package protocol

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

// ChannelParams is the ATTACH/ATTACHED channel-params map. On the wire it is
// a string→string map, but SDKs may send a value as a non-string — ably-js's
// `channels.get(name, {params: {rewind: 1}})` puts the number 1 in the map,
// so the wire carries `{"rewind": 1}` (an integer value). A plain
// map[string]string decode rejects that and drops the whole frame, so the
// server would silently never answer the ATTACH and the channel would hang
// in ATTACHING. To match the reference server's tolerant paramsFromMap, the
// custom decoders below coerce every value to its string form on ingress.
// Encoding stays the default map[string]string marshalling (values are
// always strings by the time we echo them).
type ChannelParams map[string]string

// paramValueString coerces a decoded channel-param value to the string form
// the rest of the server works with. Numbers are rendered without scientific
// notation or trailing zeros (so a JSON `1` becomes "1", not "1e+00"); other
// scalars use their natural string form.
func paramValueString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(x)
	default:
		// int8/16/32/64, uint*, etc. from the msgpack decoder — %v renders
		// each as a plain integer.
		return fmt.Sprintf("%v", x)
	}
}

func coerceParams(raw map[string]any) ChannelParams {
	if raw == nil {
		return nil
	}
	m := make(ChannelParams, len(raw))
	for k, v := range raw {
		m[k] = paramValueString(v)
	}
	return m
}

// UnmarshalJSON decodes the params object leniently, coercing non-string
// values to strings (see ChannelParams).
func (p *ChannelParams) UnmarshalJSON(b []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*p = coerceParams(raw)
	return nil
}

// DecodeMsgpack mirrors UnmarshalJSON for the msgpack transport.
func (p *ChannelParams) DecodeMsgpack(dec *msgpack.Decoder) error {
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	*p = coerceParams(raw)
	return nil
}

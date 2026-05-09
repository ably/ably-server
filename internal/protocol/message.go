package protocol

// ProtocolMessage is one frame on the realtime WebSocket connection.
type ProtocolMessage struct {
	Action        Action `json:"action"                  msgpack:"action"`
	ID            string `json:"id,omitempty"            msgpack:"id,omitempty"`
	ConnectionID  string `json:"connectionId,omitempty"  msgpack:"connectionId,omitempty"`
	Channel       string `json:"channel,omitempty"       msgpack:"channel,omitempty"`
	ChannelSerial string `json:"channelSerial,omitempty" msgpack:"channelSerial,omitempty"`
	MsgSerial     int64  `json:"msgSerial,omitempty"     msgpack:"msgSerial,omitempty"`
	Timestamp     int64  `json:"timestamp,omitempty"     msgpack:"timestamp,omitempty"`
	Count         int    `json:"count,omitempty"         msgpack:"count,omitempty"`
	Flags         int64  `json:"flags,omitempty"         msgpack:"flags,omitempty"`
}

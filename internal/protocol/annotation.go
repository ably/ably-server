package protocol

import (
	"fmt"
	"strings"
)

// AnnotationAction is the action carried by an Annotation — the
// annotation-stream analogue of MessageAction/PresenceAction. Values are
// pinned to Ably's Annotation.Action wire enum (DESIGN.md §14.1).
type AnnotationAction int8

const (
	// AnnotationCreate attaches a new annotation to an existing message.
	AnnotationCreate AnnotationAction = 0
	// AnnotationDelete removes the caller's own contribution(s) for the
	// annotation type, per the aggregation method's fold.
	AnnotationDelete AnnotationAction = 1
)

var annotationActionNames = map[AnnotationAction]string{
	AnnotationCreate: "annotation.create",
	AnnotationDelete: "annotation.delete",
}

func (a AnnotationAction) String() string {
	if name, ok := annotationActionNames[a]; ok {
		return name
	}
	return "unknown"
}

// Annotation is one annotation attached to an existing message
// (DESIGN.md §14.1). It rides the channel stream as a third cm kind
// (kind = annotation) carried in ChannelMessage.Annotations. Field names
// and enum values are pinned to Ably's wire shape.
//
// ID and Serial carry the same split as on Message (DESIGN.md §8):
//   - ID is client-supplied and optional; it carries idempotency intent.
//   - Serial is server-assigned on publish in the form
//     `<channelSerial>:<idx>`.
//
// MessageSerial is the TARGET message's identity serial — the message
// the annotation is attached to (client-supplied). Type has the form
// `<name>:<aggregation>` (see AnnotationAggregations). Action carries no
// omitempty: an annotation.create must emit action=0 on the wire to match
// Ably's SDKs.
type Annotation struct {
	ID            string           `json:"id,omitempty"            msgpack:"id,omitempty"`
	Serial        string           `json:"serial,omitempty"        msgpack:"serial,omitempty"`
	Action        AnnotationAction `json:"action"                  msgpack:"action"`
	ClientID      string           `json:"clientId,omitempty"      msgpack:"clientId,omitempty"`
	ConnectionID  string           `json:"connectionId,omitempty"  msgpack:"connectionId,omitempty"`
	Type          string           `json:"type,omitempty"          msgpack:"type,omitempty"`
	Name          string           `json:"name,omitempty"          msgpack:"name,omitempty"`
	MessageSerial string           `json:"messageSerial,omitempty" msgpack:"messageSerial,omitempty"`
	Count         int              `json:"count,omitempty"         msgpack:"count,omitempty"`
	Data          any              `json:"data,omitempty"          msgpack:"data,omitempty"`
	Encoding      string           `json:"encoding,omitempty"      msgpack:"encoding,omitempty"`
	Timestamp     int64            `json:"timestamp,omitempty"     msgpack:"timestamp,omitempty"`
	// Summary is the post-fold summary snapshot of this annotation's target
	// message, stamped by StoreAnnotation for the MESSAGE/summary (action 4)
	// delivery frame (DESIGN.md §14.2, §14.3). It is server-internal: never
	// encoded to a client and never part of the persisted annotation
	// payload (excluded from both json and msgpack). On the single-process
	// backends it rides the in-memory cm to the appender; on the Postgres
	// cluster path it is persisted in a dedicated channel_messages.summary
	// column and reconstructed here on the LISTEN load, so a node that never
	// witnessed earlier annotations emits the identical summary.
	Summary Summary `json:"-" msgpack:"-"`
}

// AnnotationAggregations is the set of v1 summarisation methods an
// annotation type's `<aggregation>` suffix must name (DESIGN.md §14.1),
// pinned to Ably's reference list.
var AnnotationAggregations = map[string]bool{
	"distinct.v1": true, // per-value set of distinct clientIds
	"unique.v1":   true, // one value per clientId, newest wins
	"multiple.v1": true, // per-value counts, count honoured
	"flag.v1":     true, // boolean per clientId
	"total.v1":    true, // anonymous tally
}

// AnnotationAnonymousAggregations is the subset of aggregation methods an
// unidentified (anonymous) client may publish (DESIGN.md §14.1), mirroring
// Ably's AllowedAnonymousAggregationMethods: multiple.v1 (reactions that
// mix identified and anonymous) and total.v1 (anonymous tally).
var AnnotationAnonymousAggregations = map[string]bool{
	"multiple.v1": true,
	"total.v1":    true,
}

// annotationAggregationsUsingCount lists the methods for which Count is
// meaningful; a zero/unspecified count defaults to 1 (Ably PSRFC-13).
var annotationAggregationsUsingCount = map[string]bool{
	"multiple.v1": true,
}

// AnnotationError is a validation failure of an inbound annotation. Its
// Code/StatusCode mirror Ably's ablyrpc.Annotation.Validate error shapes
// (all 40000 / 400); callers surface it as a NACK (WS) or 400 (REST).
type AnnotationError struct {
	Message    string
	Code       int
	StatusCode int
}

func (e *AnnotationError) Error() string { return e.Message }

// ErrorInfo renders the AnnotationError as the wire ErrorInfo carried on a
// NACK / REST error body.
func (e *AnnotationError) ErrorInfo() *ErrorInfo {
	return &ErrorInfo{Message: e.Message, Code: e.Code, StatusCode: e.StatusCode}
}

// ParseAnnotationType splits an annotation type into its `<name>` and
// `<aggregation>` parts (DESIGN.md §14.1). The aggregation is empty for an
// unaggregated type. ok is false for a malformed type (empty name, or more
// than one ':' separator).
func ParseAnnotationType(typ string) (name, aggregation string, ok bool) {
	if typ == "" {
		return "", "", false
	}
	name, aggregation, found := strings.Cut(typ, ":")
	if name == "" {
		return "", "", false
	}
	if found && strings.Contains(aggregation, ":") {
		return "", "", false
	}
	return name, aggregation, true
}

// Validate applies the §14.1 annotation validation, mirroring Ably's
// ablyrpc.Annotation.Validate: a missing MessageSerial or Type is rejected
// (40000), a malformed type or unknown aggregation is rejected, and an
// anonymous (empty ClientID) publish is confined to the anonymous-allowed
// aggregation methods. For multiple.v1 a zero Count defaults to 1. It
// returns nil when the annotation is valid, mutating Count where defaulted.
func (a *Annotation) Validate() *AnnotationError {
	if a.MessageSerial == "" {
		return &AnnotationError{Message: "annotations must reference a message (missing messageSerial)", Code: 40000, StatusCode: 400}
	}
	if a.Type == "" {
		return &AnnotationError{Message: "annotations must include a type", Code: 40000, StatusCode: 400}
	}
	_, aggregation, ok := ParseAnnotationType(a.Type)
	if !ok {
		return &AnnotationError{Message: "malformed annotation type", Code: 40000, StatusCode: 400}
	}
	if aggregation == "" {
		// Unaggregated annotation — no method to validate.
		return nil
	}
	if !AnnotationAggregations[aggregation] {
		return &AnnotationError{Message: fmt.Sprintf("unknown annotation aggregation type: %s", aggregation), Code: 40000, StatusCode: 400}
	}
	if a.ClientID == "" && !AnnotationAnonymousAggregations[aggregation] {
		return &AnnotationError{Message: "aggregation method not allowed for anonymous client publish; allowed methods are: multiple.v1, total.v1", Code: 40000, StatusCode: 400}
	}
	if annotationAggregationsUsingCount[aggregation] && a.Count == 0 {
		// A count of 0 (or unspecified — Go's decode can't tell them apart)
		// is not valid; default to 1 rather than reject (Ably PSRFC-13).
		a.Count = 1
	}
	return nil
}

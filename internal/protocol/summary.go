package protocol

import (
	"encoding/json"
	"slices"

	"github.com/vmihailenco/msgpack/v5"
)

// A Summary is the per-message fold of that message's annotations
// (DESIGN.md §14.2), keyed by the full annotation type
// (`<name>:<aggregation>`). Each Aggregation is shaped by the type's
// aggregation method. It rides the message projection (so reads carry the
// current summary) and the MESSAGE/summary (action 4) delivery frame.
//
// The wire shape is method-specific and un-wrapped, pinned to Ably's:
// distinct.v1 / unique.v1 encode as `{value: {total, clientIds:[…]}}`,
// multiple.v1 as `{value: {total, clientIds:{id:count}, totalUnidentified}}`,
// flag.v1 as a single `{total, clientIds:[…]}`, total.v1 as `{total}`. The
// Summary map itself carries the codec (Summary.MarshalJSON /
// EncodeMsgpack and their inverses) because only it holds the type key
// that names the method — an Aggregation cannot decode itself.
type Summary map[string]*Aggregation

// Aggregation is one entry in a Summary: the rolled-up annotations of a
// single type, shaped by that type's aggregation method. Exactly one of
// the typed fields is populated, selected by Method (the aggregation
// suffix of the annotation type, e.g. "distinct.v1").
type Aggregation struct {
	// Method is the aggregation suffix (distinct.v1 / unique.v1 /
	// multiple.v1 / flag.v1 / total.v1) that selects the populated field.
	Method string
	// Values backs distinct.v1 and unique.v1: per-value sets of the
	// distinct client ids that annotated with that value.
	Values map[string]*ClientIDList
	// Counts backs multiple.v1: per-value tallies of client id counts.
	Counts map[string]*ClientIDCounts
	// Flag backs flag.v1: the set of clients that set the flag.
	Flag *ClientIDList
	// Total backs total.v1: an anonymous tally.
	Total *TotalAggregation
}

// ClientIDList is a de-duplicated, insertion-ordered set of the client ids
// that contributed to a distinct/unique value or set a flag, plus the
// total count (DESIGN.md §14.2). Total equals len(ClientIDs) for the folds
// implemented here (no unidentified contributions reach a ClientIDList).
type ClientIDList struct {
	Total     int      `json:"total"               msgpack:"total"`
	ClientIDs []string `json:"clientIds,omitempty" msgpack:"clientIds,omitempty"`
	Clipped   bool     `json:"clipped,omitempty"   msgpack:"clipped,omitempty"`
}

// ClientIDCounts is the multiple.v1 per-value tally: a sum of counts from
// all annotations (Total), the per-identified-client sums (ClientIDs), the
// sum from anonymous clients (TotalUnidentified), and the number of
// distinct identified clients (TotalClientIDs). Total and TotalUnidentified
// carry no omitempty so a value with only anonymous contributions still
// reports totalUnidentified, matching Ably.
type ClientIDCounts struct {
	Total             int            `json:"total"                    msgpack:"total"`
	ClientIDs         map[string]int `json:"clientIds,omitempty"      msgpack:"clientIds,omitempty"`
	TotalUnidentified int            `json:"totalUnidentified"        msgpack:"totalUnidentified"`
	Clipped           bool           `json:"clipped,omitempty"        msgpack:"clipped,omitempty"`
	TotalClientIDs    int            `json:"totalClientIds,omitempty" msgpack:"totalClientIds,omitempty"`
}

// TotalAggregation is the total.v1 anonymous tally.
type TotalAggregation struct {
	Total int `json:"total" msgpack:"total"`
}

// inner returns the method-specific value that encodes as the aggregation's
// un-wrapped wire shape.
func (a *Aggregation) inner() any {
	switch a.Method {
	case "distinct.v1", "unique.v1":
		return a.Values
	case "multiple.v1":
		return a.Counts
	case "flag.v1":
		return a.Flag
	case "total.v1":
		return a.Total
	default:
		return nil
	}
}

// empty reports whether the aggregation holds no contributions and should
// be dropped from the summary.
func (a *Aggregation) empty() bool {
	switch a.Method {
	case "distinct.v1", "unique.v1":
		return len(a.Values) == 0
	case "multiple.v1":
		return len(a.Counts) == 0
	case "flag.v1":
		return a.Flag == nil || len(a.Flag.ClientIDs) == 0
	case "total.v1":
		return a.Total == nil || a.Total.Total <= 0
	default:
		return true
	}
}

// clone deep-copies the aggregation so a fold never mutates a prior
// snapshot shared with a delivered annotation cm.
func (a *Aggregation) clone() *Aggregation {
	if a == nil {
		return nil
	}
	c := &Aggregation{Method: a.Method}
	if a.Values != nil {
		c.Values = make(map[string]*ClientIDList, len(a.Values))
		for k, v := range a.Values {
			c.Values[k] = &ClientIDList{Total: v.Total, ClientIDs: slices.Clone(v.ClientIDs), Clipped: v.Clipped}
		}
	}
	if a.Counts != nil {
		c.Counts = make(map[string]*ClientIDCounts, len(a.Counts))
		for k, v := range a.Counts {
			cc := &ClientIDCounts{Total: v.Total, TotalUnidentified: v.TotalUnidentified, Clipped: v.Clipped, TotalClientIDs: v.TotalClientIDs}
			if v.ClientIDs != nil {
				cc.ClientIDs = make(map[string]int, len(v.ClientIDs))
				for id, n := range v.ClientIDs {
					cc.ClientIDs[id] = n
				}
			}
			c.Counts[k] = cc
		}
	}
	if a.Flag != nil {
		c.Flag = &ClientIDList{Total: a.Flag.Total, ClientIDs: slices.Clone(a.Flag.ClientIDs), Clipped: a.Flag.Clipped}
	}
	if a.Total != nil {
		c.Total = &TotalAggregation{Total: a.Total.Total}
	}
	return c
}

// Clone returns a deep copy of the summary. StoreAnnotation stamps a clone
// onto each annotation cm so its delivered snapshot is independent of later
// folds in the same batch (DESIGN.md §14.2).
func (s Summary) Clone() Summary {
	if s == nil {
		return nil
	}
	c := make(Summary, len(s))
	for typ, agg := range s {
		c[typ] = agg.clone()
	}
	return c
}

// Apply folds one annotation into the summary and returns the result. It
// treats the receiver as immutable — the fold runs against a clone — so the
// pre-fold summary (already stamped on an earlier annotation cm) is never
// disturbed. The annotation type names the method; an unaggregated or
// unknown-method type is ignored (returns the summary unchanged). An
// aggregation that folds down to nothing is dropped from the map.
func (s Summary) Apply(a *Annotation) Summary {
	_, method, ok := ParseAnnotationType(a.Type)
	if !ok || method == "" {
		return s
	}
	out := s.Clone()
	if out == nil {
		out = make(Summary)
	}
	agg := out[a.Type]
	if agg == nil {
		agg = &Aggregation{Method: method}
	}
	// The per-value key is the annotation's Name (e.g. "👍"), distinct from
	// the type's name part; flag.v1 and total.v1 ignore it.
	switch method {
	case "distinct.v1":
		foldDistinct(agg, a, a.Name)
	case "unique.v1":
		foldUnique(agg, a, a.Name)
	case "multiple.v1":
		foldMultiple(agg, a, a.Name)
	case "flag.v1":
		foldFlag(agg, a)
	case "total.v1":
		foldTotal(agg, a)
	default:
		return s
	}
	if agg.empty() {
		delete(out, a.Type)
	} else {
		out[a.Type] = agg
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// foldDistinct: per-value set of the distinct client ids that annotated
// with that value. A repeat by the same client is a no-op; a delete removes
// the client from that value's set (DESIGN.md §14.2).
func foldDistinct(agg *Aggregation, a *Annotation, value string) {
	if agg.Values == nil {
		agg.Values = make(map[string]*ClientIDList)
	}
	switch a.Action {
	case AnnotationCreate:
		lst := agg.Values[value]
		if lst == nil {
			lst = &ClientIDList{}
			agg.Values[value] = lst
		}
		addClientID(lst, a.ClientID)
	case AnnotationDelete:
		if lst := agg.Values[value]; lst != nil {
			removeClientID(lst, a.ClientID)
			if len(lst.ClientIDs) == 0 {
				delete(agg.Values, value)
			}
		}
	}
}

// foldUnique: one value per client, newest wins. A create moves the client
// to the annotated value (removing them from any other); a delete removes
// the client from that value if it is their current one (DESIGN.md §14.2).
func foldUnique(agg *Aggregation, a *Annotation, value string) {
	if agg.Values == nil {
		agg.Values = make(map[string]*ClientIDList)
	}
	switch a.Action {
	case AnnotationCreate:
		for v, lst := range agg.Values {
			if v == value {
				continue
			}
			removeClientID(lst, a.ClientID)
			if len(lst.ClientIDs) == 0 {
				delete(agg.Values, v)
			}
		}
		lst := agg.Values[value]
		if lst == nil {
			lst = &ClientIDList{}
			agg.Values[value] = lst
		}
		addClientID(lst, a.ClientID)
	case AnnotationDelete:
		if lst := agg.Values[value]; lst != nil {
			removeClientID(lst, a.ClientID)
			if len(lst.ClientIDs) == 0 {
				delete(agg.Values, value)
			}
		}
	}
}

// foldMultiple: per-value counts, honouring Count and permitting anonymous
// contributions (summed into TotalUnidentified). A delete subtracts the
// caller's count (DESIGN.md §14.2).
func foldMultiple(agg *Aggregation, a *Annotation, value string) {
	if agg.Counts == nil {
		agg.Counts = make(map[string]*ClientIDCounts)
	}
	count := a.Count
	if count == 0 {
		count = 1
	}
	cc := agg.Counts[value]
	if cc == nil {
		if a.Action == AnnotationDelete {
			return
		}
		cc = &ClientIDCounts{}
		agg.Counts[value] = cc
	}
	delta := count
	if a.Action == AnnotationDelete {
		delta = -count
	}
	cc.Total += delta
	if a.ClientID == "" {
		cc.TotalUnidentified += delta
	} else {
		if cc.ClientIDs == nil {
			cc.ClientIDs = make(map[string]int)
		}
		cc.ClientIDs[a.ClientID] += delta
		if cc.ClientIDs[a.ClientID] <= 0 {
			delete(cc.ClientIDs, a.ClientID)
		}
	}
	cc.TotalClientIDs = len(cc.ClientIDs)
	if cc.Total <= 0 && len(cc.ClientIDs) == 0 && cc.TotalUnidentified <= 0 {
		delete(agg.Counts, value)
	}
}

// foldFlag: a boolean per client, independent of the annotation's Name. A
// create adds the client, a delete removes them (DESIGN.md §14.2).
func foldFlag(agg *Aggregation, a *Annotation) {
	if agg.Flag == nil {
		agg.Flag = &ClientIDList{}
	}
	switch a.Action {
	case AnnotationCreate:
		addClientID(agg.Flag, a.ClientID)
	case AnnotationDelete:
		removeClientID(agg.Flag, a.ClientID)
	}
}

// foldTotal: an anonymous tally, one per annotation (Count is not honoured
// — only multiple.v1 uses it). A delete decrements (DESIGN.md §14.2).
func foldTotal(agg *Aggregation, a *Annotation) {
	if agg.Total == nil {
		agg.Total = &TotalAggregation{}
	}
	switch a.Action {
	case AnnotationCreate:
		agg.Total.Total++
	case AnnotationDelete:
		agg.Total.Total--
	}
}

// addClientID appends id to the list if absent (insertion order preserved)
// and refreshes Total to the distinct-client count.
func addClientID(lst *ClientIDList, id string) {
	if !slices.Contains(lst.ClientIDs, id) {
		lst.ClientIDs = append(lst.ClientIDs, id)
	}
	lst.Total = len(lst.ClientIDs)
}

// removeClientID drops id from the list if present and refreshes Total.
func removeClientID(lst *ClientIDList, id string) {
	if i := slices.Index(lst.ClientIDs, id); i >= 0 {
		lst.ClientIDs = slices.Delete(lst.ClientIDs, i, i+1)
	}
	lst.Total = len(lst.ClientIDs)
}

// MarshalJSON encodes the summary as {type: <method-shaped object>}.
func (s Summary) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.innerMap())
}

// EncodeMsgpack encodes the summary as a msgpack map of the same shape.
func (s Summary) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(s.innerMap())
}

func (s Summary) innerMap() map[string]any {
	m := make(map[string]any, len(s))
	for typ, agg := range s {
		m[typ] = agg.inner()
	}
	return m
}

// UnmarshalJSON decodes a {type: <method-shaped object>} map, dispatching
// each entry on the type's aggregation-method suffix. Unknown methods are
// skipped (matching Ably's summary decode).
func (s *Summary) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	out := make(Summary, len(raw))
	for typ, r := range raw {
		agg, err := decodeAggregation(typ, r, json.Unmarshal)
		if err != nil {
			return err
		}
		if agg != nil {
			out[typ] = agg
		}
	}
	*s = out
	return nil
}

// DecodeMsgpack decodes the msgpack summary map, dispatching per entry on
// the type's aggregation-method suffix.
func (s *Summary) DecodeMsgpack(dec *msgpack.Decoder) error {
	var raw map[string]msgpack.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	out := make(Summary, len(raw))
	for typ, r := range raw {
		agg, err := decodeAggregation(typ, []byte(r), msgpack.Unmarshal)
		if err != nil {
			return err
		}
		if agg != nil {
			out[typ] = agg
		}
	}
	*s = out
	return nil
}

// decodeAggregation builds an Aggregation from the raw inner bytes for one
// summary entry, using unmarshal (json.Unmarshal or msgpack.Unmarshal) to
// decode the method-specific shape. A nil result means an unknown method
// that the caller should skip.
func decodeAggregation(typ string, raw []byte, unmarshal func([]byte, any) error) (*Aggregation, error) {
	_, method, ok := ParseAnnotationType(typ)
	if !ok || method == "" {
		return nil, nil
	}
	agg := &Aggregation{Method: method}
	switch method {
	case "distinct.v1", "unique.v1":
		if err := unmarshal(raw, &agg.Values); err != nil {
			return nil, err
		}
	case "multiple.v1":
		if err := unmarshal(raw, &agg.Counts); err != nil {
			return nil, err
		}
	case "flag.v1":
		if err := unmarshal(raw, &agg.Flag); err != nil {
			return nil, err
		}
	case "total.v1":
		if err := unmarshal(raw, &agg.Total); err != nil {
			return nil, err
		}
	default:
		return nil, nil
	}
	return agg, nil
}

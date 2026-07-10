package protocol

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestSummaryJSONEncoding pins the per-method summary wire shapes,
// transcribed from the reference's ablyrpc TestMessageSummary_JSONEncoding
// (DESIGN.md §14.2/§14.3, TASK-66 AC#4). Each case marshals a summary and
// asserts the exact JSON, then round-trips it back.
func TestSummaryJSONEncoding(t *testing.T) {
	tests := []struct {
		name     string
		summary  Summary
		expected string
	}{
		{
			name: "distinct.v1",
			summary: Summary{
				"reaction:distinct.v1": {
					Method: "distinct.v1",
					Values: map[string]*ClientIDList{
						"👍": {Total: 3, ClientIDs: []string{"user1", "user2", "user3"}},
						"👎": {Total: 2, ClientIDs: []string{"user1", "user2"}},
					},
				},
			},
			expected: `{
				"reaction:distinct.v1": {
					"👍": {"total": 3, "clientIds": ["user1", "user2", "user3"]},
					"👎": {"total": 2, "clientIds": ["user1", "user2"]}
				}
			}`,
		},
		{
			name: "unique.v1",
			summary: Summary{
				"reaction:unique.v1": {
					Method: "unique.v1",
					Values: map[string]*ClientIDList{
						"👍": {Total: 3, ClientIDs: []string{"user1", "user2", "user3"}},
						"👎": {Total: 2, ClientIDs: []string{"user4", "user5"}},
					},
				},
			},
			expected: `{
				"reaction:unique.v1": {
					"👍": {"total": 3, "clientIds": ["user1", "user2", "user3"]},
					"👎": {"total": 2, "clientIds": ["user4", "user5"]}
				}
			}`,
		},
		{
			name: "multiple.v1",
			summary: Summary{
				"reaction:multiple.v1": {
					Method: "multiple.v1",
					Counts: map[string]*ClientIDCounts{
						"👍": {Total: 6, ClientIDs: map[string]int{"user1": 1, "user2": 2}, TotalUnidentified: 3},
						"👎": {Total: 2, ClientIDs: map[string]int{"user1": 1, "user2": 1}},
					},
				},
			},
			expected: `{
				"reaction:multiple.v1": {
					"👍": {"total": 6, "clientIds": {"user1": 1, "user2": 2}, "totalUnidentified": 3},
					"👎": {"total": 2, "clientIds": {"user1": 1, "user2": 1}, "totalUnidentified": 0}
				}
			}`,
		},
		{
			name: "flag.v1",
			summary: Summary{
				"moderate:flag.v1": {
					Method: "flag.v1",
					Flag:   &ClientIDList{Total: 4, ClientIDs: []string{"user1", "user2", "user3", "user4"}},
				},
			},
			expected: `{
				"moderate:flag.v1": {"total": 4, "clientIds": ["user1", "user2", "user3", "user4"]}
			}`,
		},
		{
			name: "total.v1",
			summary: Summary{
				"reaction:total.v1": {
					Method: "total.v1",
					Total:  &TotalAggregation{Total: 5},
				},
			},
			expected: `{"reaction:total.v1": {"total": 5}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.summary)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			assertJSONEq(t, tc.expected, string(got))

			// Round-trip back to a typed Summary.
			var back Summary
			if err := json.Unmarshal(got, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(tc.summary, back) {
				t.Errorf("json round-trip mismatch:\n got %#v\nwant %#v", back, tc.summary)
			}
		})
	}
}

// TestSummaryMsgpackRoundTrip pins the msgpack persistence path (the
// projection payload and the pg summary snapshot column, DESIGN.md §14.2):
// a summary embedded on a Message must survive a msgpack round-trip typed.
func TestSummaryMsgpackRoundTrip(t *testing.T) {
	m := &Message{
		Serial: "00000000000001-000@abc:000",
		Action: MessageSummary,
		Summary: Summary{
			"reaction:distinct.v1": {
				Method: "distinct.v1",
				Values: map[string]*ClientIDList{"👍": {Total: 1, ClientIDs: []string{"user1"}}},
			},
			"reaction:multiple.v1": {
				Method: "multiple.v1",
				Counts: map[string]*ClientIDCounts{"👍": {Total: 2, ClientIDs: map[string]int{"user1": 2}, TotalClientIDs: 1}},
			},
			"reaction:total.v1": {Method: "total.v1", Total: &TotalAggregation{Total: 3}},
			"moderate:flag.v1":  {Method: "flag.v1", Flag: &ClientIDList{Total: 1, ClientIDs: []string{"user1"}}},
		},
	}
	blob, err := msgpack.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Message
	if err := msgpack.Unmarshal(blob, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(m.Summary, got.Summary) {
		t.Errorf("msgpack round-trip mismatch:\n got %#v\nwant %#v", got.Summary, m.Summary)
	}
}

// TestSummaryFold exercises the five folds against the exact annotation
// sequence and expected summary from the reference qos annotations test
// (roles/qos/annotations.go), the authoritative fold semantics incl.
// annotation.delete handling per method (TASK-66 AC#4).
func TestSummaryFold(t *testing.T) {
	// The reference qos sequence, per type. See roles/qos/annotations.go.
	seq := []*Annotation{
		// distinct: user1 👍+👎, user2 👍 twice, user3 ❌ then deleted.
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:distinct.v1", Name: "👍"},
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:distinct.v1", Name: "👎"},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:distinct.v1", Name: "👍"},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:distinct.v1", Name: "👍"},
		{Action: AnnotationCreate, ClientID: "user3", Type: "qos:distinct.v1", Name: "❌"},
		{Action: AnnotationDelete, ClientID: "user3", Type: "qos:distinct.v1", Name: "❌"},
		// unique: user1 👍 then 👎 (moves), user2 👍 twice, user3 ❌ deleted.
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:unique.v1", Name: "👍"},
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:unique.v1", Name: "👎"},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:unique.v1", Name: "👍"},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:unique.v1", Name: "👍"},
		{Action: AnnotationCreate, ClientID: "user3", Type: "qos:unique.v1", Name: "❌"},
		{Action: AnnotationDelete, ClientID: "user3", Type: "qos:unique.v1", Name: "❌"},
		// multiple: user1 👍×1 👎×1, anon 👎×1, user2 👍×1 then 👍×2, user3 ❌×1 deleted.
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:multiple.v1", Name: "👍", Count: 1},
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:multiple.v1", Name: "👎", Count: 1},
		{Action: AnnotationCreate, Type: "qos:multiple.v1", Name: "👎", Count: 1},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:multiple.v1", Name: "👍", Count: 1},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:multiple.v1", Name: "👍", Count: 2},
		{Action: AnnotationCreate, ClientID: "user3", Type: "qos:multiple.v1", Name: "❌", Count: 1},
		{Action: AnnotationDelete, ClientID: "user3", Type: "qos:multiple.v1", Name: "❌", Count: 1},
		// flag: user1, user2 set flag, user3 sets then deletes.
		{Action: AnnotationCreate, ClientID: "user1", Type: "qos:flag.v1"},
		{Action: AnnotationCreate, ClientID: "user2", Type: "qos:flag.v1"},
		{Action: AnnotationCreate, ClientID: "user3", Type: "qos:flag.v1", Name: "❌"},
		{Action: AnnotationDelete, ClientID: "user3", Type: "qos:flag.v1", Name: "❌"},
		// total: 5 creates, 1 delete -> 4.
		{Action: AnnotationCreate, Type: "qos:total.v1"},
		{Action: AnnotationCreate, Type: "qos:total.v1"},
		{Action: AnnotationCreate, Type: "qos:total.v1"},
		{Action: AnnotationCreate, Type: "qos:total.v1"},
		{Action: AnnotationCreate, Type: "qos:total.v1"},
		{Action: AnnotationDelete, Type: "qos:total.v1"},
	}

	var s Summary
	for _, a := range seq {
		s = s.Apply(a)
	}

	want := Summary{
		"qos:distinct.v1": {
			Method: "distinct.v1",
			Values: map[string]*ClientIDList{
				"👍": {Total: 2, ClientIDs: []string{"user1", "user2"}},
				"👎": {Total: 1, ClientIDs: []string{"user1"}},
			},
		},
		"qos:unique.v1": {
			Method: "unique.v1",
			Values: map[string]*ClientIDList{
				"👍": {Total: 1, ClientIDs: []string{"user2"}},
				"👎": {Total: 1, ClientIDs: []string{"user1"}},
			},
		},
		"qos:multiple.v1": {
			Method: "multiple.v1",
			Counts: map[string]*ClientIDCounts{
				"👍": {Total: 4, ClientIDs: map[string]int{"user1": 1, "user2": 3}, TotalUnidentified: 0, TotalClientIDs: 2},
				"👎": {Total: 2, ClientIDs: map[string]int{"user1": 1}, TotalUnidentified: 1, TotalClientIDs: 1},
			},
		},
		"qos:flag.v1": {
			Method: "flag.v1",
			Flag:   &ClientIDList{Total: 2, ClientIDs: []string{"user1", "user2"}},
		},
		"qos:total.v1": {
			Method: "total.v1",
			Total:  &TotalAggregation{Total: 4},
		},
	}
	if !reflect.DeepEqual(want, s) {
		t.Errorf("fold mismatch:\n got %#v\nwant %#v", s, want)
	}
}

// TestSummaryApplyIsImmutable verifies Apply does not mutate the receiver,
// so a snapshot already stamped on a delivered annotation cm is never
// disturbed by a later fold in the same batch (DESIGN.md §14.2).
func TestSummaryApplyIsImmutable(t *testing.T) {
	s0 := Summary(nil).Apply(&Annotation{Action: AnnotationCreate, ClientID: "user1", Type: "r:distinct.v1", Name: "👍"})
	snapshot := s0.Clone()
	_ = s0.Apply(&Annotation{Action: AnnotationCreate, ClientID: "user2", Type: "r:distinct.v1", Name: "👍"})
	if !reflect.DeepEqual(snapshot, s0) {
		t.Errorf("Apply mutated its receiver:\n got %#v\nwant %#v", s0, snapshot)
	}
}

// TestSummaryEmptyFoldsDropType verifies an aggregation that folds down to
// nothing is dropped from the summary map.
func TestSummaryEmptyFoldsDropType(t *testing.T) {
	var s Summary
	s = s.Apply(&Annotation{Action: AnnotationCreate, ClientID: "user1", Type: "r:flag.v1"})
	s = s.Apply(&Annotation{Action: AnnotationDelete, ClientID: "user1", Type: "r:flag.v1"})
	if s != nil {
		t.Errorf("summary = %#v, want nil after the only contribution was removed", s)
	}
}

func assertJSONEq(t *testing.T, want, got string) {
	t.Helper()
	var wv, gv any
	if err := json.Unmarshal([]byte(want), &wv); err != nil {
		t.Fatalf("bad want JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(got), &gv); err != nil {
		t.Fatalf("bad got JSON: %v", err)
	}
	if !reflect.DeepEqual(wv, gv) {
		t.Errorf("JSON mismatch:\n got %s\nwant %s", got, want)
	}
}

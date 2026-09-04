package auth

import "testing"

func TestMatchResource(t *testing.T) {
	cases := []struct {
		pattern, channel string
		want             bool
	}{
		// "*" matches every channel.
		{"*", "foo", true},
		{"*", "foo:bar:baz", true},
		// Trailing wildcard matches any number of trailing segments,
		// including none (mirrors Ably's pathsMatch).
		{"foo:*", "foo:bar", true},
		{"foo:*", "foo:bar:baz", true},
		{"foo:*", "foo", true},
		{"foo:*", "bar:baz", false},
		{"foo:*", "foobar", false},
		// Interior wildcard matches exactly one segment.
		{"foo:*:baz", "foo:bar:baz", true},
		{"foo:*:baz", "foo:bar:bam:baz", false},
		{"foo:*:baz", "foo:baz", false},
		// A literal pattern (no bare "*" segment) matches only itself,
		// including a trailing '*' character which is NOT a wildcard.
		{"foo", "foo", true},
		{"foo", "foo:bar", false},
		{"foo*", "foo*", true},
		{"foo*", "foobar", false},
		{"foo*", "foo", false},
		// A "[qualifier]" prefix scopes the resource TYPE. "[*]" matches
		// any type, so it matches a plain channel; the name path then
		// applies the usual wildcard rules.
		{"[*]*", "foo", true},
		{"[*]*", "foo:bar:baz", true},
		{"[*]foo", "foo", true},
		{"[*]foo", "bar", false},
		{"[*]chat:*", "chat:room", true},
		{"[*]chat:*", "other", false},
		// A concrete qualifier matches no plain channel: queues and
		// metachannels do not exist as resources here.
		{"[meta]*", "foo", false},
		{"[queue]*", "foo", false},
		{"[meta]log", "log", false},
	}
	for _, tc := range cases {
		if got := matchResource(tc.pattern, tc.channel); got != tc.want {
			t.Errorf("matchResource(%q, %q) = %v, want %v", tc.pattern, tc.channel, got, tc.want)
		}
	}
}

func TestCapabilityPermits(t *testing.T) {
	cap, err := ParseCapability(`{"*":["*"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !cap.Permits("anything", OpPublish) || !cap.Permits("a:b:c", OpHistory) {
		t.Errorf("full capability should permit every op on every channel")
	}

	// Op union across multiple matching resources.
	multi, err := ParseCapability(`{"chat:*":["subscribe"],"*":["history"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !multi.Permits("chat:room", OpSubscribe) {
		t.Errorf("chat:* should grant subscribe on chat:room")
	}
	if !multi.Permits("chat:room", OpHistory) {
		t.Errorf("* should grant history on chat:room (union across resources)")
	}
	if multi.Permits("chat:room", OpPublish) {
		t.Errorf("publish should not be granted on chat:room")
	}
	if multi.Permits("other", OpSubscribe) {
		t.Errorf("subscribe should not be granted on 'other' (chat:* does not match)")
	}
	if !multi.Permits("other", OpHistory) {
		t.Errorf("* should grant history on 'other'")
	}

	// A narrow single-op capability.
	narrow, _ := ParseCapability(`{"news:*":["publish"]}`)
	if !narrow.Permits("news:sport", OpPublish) {
		t.Errorf("news:* should grant publish on news:sport")
	}
	if narrow.Permits("news:sport", OpSubscribe) {
		t.Errorf("subscribe should not be granted")
	}
	if narrow.Permits("weather", OpPublish) {
		t.Errorf("publish should not be granted on weather")
	}

	// The sandbox all-access key keys[5] — the exact capability string
	// from cmd/ably-local-sandbox/testdata/test-app-setup.json — grants every
	// channel op on an arbitrary channel (the AIT-suite blocker).
	// Before the qualifier fix "[*]*" matched nothing.
	allAccess, err := ParseCapability(`{ "[*]*":["*"] }`)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range []string{"foo", "mutable:foo", "persisted:presence_fixtures"} {
		for _, op := range []Op{OpPublish, OpSubscribe, OpHistory, OpPresence} {
			if !allAccess.Permits(ch, op) {
				t.Errorf("keys[5] [*]* should grant %q on %q", op, ch)
			}
		}
	}

	// Deny-all: a zero-value Capability grants nothing.
	var zero Capability
	if zero.Permits("foo", OpPublish) {
		t.Errorf("zero-value capability should deny everything")
	}

	// The four ownership-scoped mutation ops parse and resolve.
	mut, err := ParseCapability(`{"doc:*":["message-update-own","message-delete-any"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !mut.Permits("doc:1", OpMessageUpdateOwn) || !mut.Permits("doc:1", OpMessageDeleteAny) {
		t.Errorf("message-* ops should parse and resolve")
	}
	if mut.Permits("doc:1", OpMessageUpdateAny) || mut.Permits("doc:1", OpMessageDeleteOwn) {
		t.Errorf("only the granted mutation ops should resolve")
	}
}

func TestCapabilityIntersect(t *testing.T) {
	cases := []struct {
		name, left, right, want string
	}{
		{"full ∩ full", `{"*":["*"]}`, `{"*":["*"]}`, `{"*":["*"]}`},
		{"full ∩ narrow passes narrow through", `{"chat:*":["publish"]}`, `{"*":["*"]}`, `{"chat:*":["publish"]}`},
		{"trailing star absorbs", `{"a:*":["*"]}`, `{"a:b":["publish"]}`, `{"a:b":["publish"]}`},
		{"interior star matches one", `{"a:*:c":["publish","subscribe"]}`, `{"a:b:c":["publish"]}`, `{"a:b:c":["publish"]}`},
		{"op intersection", `{"a":["publish","subscribe"]}`, `{"a":["subscribe","history"]}`, `{"a":["subscribe"]}`},
		// Token narrowing (§3.3) against the sandbox all-access key: the
		// "[*]" wildcard qualifier yields the other side's, so narrowing
		// keys[5] to a concrete requested capability keeps working.
		{"[*]* ∩ concrete request", `{"[*]*":["*"]}`, `{"chat:*":["publish","subscribe"]}`, `{"chat:*":["publish","subscribe"]}`},
		{"concrete request ∩ [*]*", `{"chat:*":["publish"]}`, `{"[*]*":["*"]}`, `{"chat:*":["publish"]}`},
		{"[*]* ∩ [*]*", `{"[*]*":["*"]}`, `{"[*]*":["*"]}`, `{"[*]*":["*"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, err := ParseCapability(tc.left)
			if err != nil {
				t.Fatal(err)
			}
			r, err := ParseCapability(tc.right)
			if err != nil {
				t.Fatal(err)
			}
			if got := l.Intersect(r).String(); got != tc.want {
				t.Errorf("Intersect = %s, want %s", got, tc.want)
			}
		})
	}

	// Empty intersection: disjoint ops or non-matching resources.
	l, _ := ParseCapability(`{"a":["publish"]}`)
	r, _ := ParseCapability(`{"a":["subscribe"]}`)
	if got := l.Intersect(r); !got.IsEmpty() {
		t.Errorf("disjoint ops should intersect empty, got %s", got)
	}
	// A leading/interior-only star cannot collapse to a shorter path.
	l2, _ := ParseCapability(`{"*:c":["publish"]}`)
	r2, _ := ParseCapability(`{"a:b:c":["publish"]}`)
	if got := l2.Intersect(r2); !got.IsEmpty() {
		t.Errorf("*:c ∩ a:b:c should be empty, got %s", got)
	}
	// Two differing concrete qualifiers cannot both be satisfied.
	l3, _ := ParseCapability(`{"[meta]*":["subscribe"]}`)
	r3, _ := ParseCapability(`{"[queue]*":["subscribe"]}`)
	if got := l3.Intersect(r3); !got.IsEmpty() {
		t.Errorf("[meta]* ∩ [queue]* should be empty, got %s", got)
	}
}

func TestParseCapabilityRejectsMalformed(t *testing.T) {
	if _, err := ParseCapability(`not json`); err == nil {
		t.Errorf("malformed capability should error")
	}
}

// TestValidateCapability covers the shape of a client-supplied requested
// capability. What is well-formed is the shared parser's judgement, so these
// pin what this server passes on to it — including the two shapes it is
// lenient about, which a token request is granted rather than refused.
func TestValidateCapability(t *testing.T) {
	cases := []struct {
		name    string
		cap     string
		wantErr bool
	}{
		{"one op", `{"foo":["publish"]}`, false},
		{"wildcard op", `{"foo":["*"]}`, false},
		{"wildcard resource", `{"*":["*"]}`, false},
		{"nothing granted", `{}`, false},

		// Granted rather than refused: an empty op list grants nothing on the
		// resource, and a wildcard alongside other ops collapses to the
		// wildcard, which was asked for anyway. Neither grants more than the
		// request did.
		{"resource with no ops", `{"foo":[]}`, false},
		{"wildcard mixed with an op", `{"foo":["*","publish"]}`, false},

		{"unrecognised op", `{"foo":["fly"]}`, true},
		{"not an object", `["publish"]`, true},
		{"not json", `{`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCapability(tc.cap)
			if tc.wantErr && err == nil {
				t.Errorf("ValidateCapability(%s) = nil, want an error", tc.cap)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("ValidateCapability(%s) = %v, want nil", tc.cap, err)
			}
		})
	}
}

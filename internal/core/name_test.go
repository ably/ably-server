package core

import "testing"

func TestValidChannelName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"room", true},
		{"my-channel_1", true},
		{"a:b:c", true},      // colons allowed after the first rune
		{"foo[bar]", true},   // bracket allowed after the first rune
		{"日本語", true},        // arbitrary non-line-break runes
		{"foo bar", true},    // interior space allowed; only leading whitespace is barred
		{"", false},          // empty
		{":hell", false},     // leading colon
		{"::", false},        // leading colon
		{"[meta]log", false}, // leading bracket (qualified channels unsupported)
		{",foo", false},      // leading comma
		{" foo", false},      // leading space
		{"\tfoo", false},     // leading tab
		{"foo\nbar", false},  // embedded line break
	}
	for _, tc := range cases {
		if got := ValidChannelName(tc.name); got != tc.want {
			t.Errorf("ValidChannelName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

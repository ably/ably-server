package core

import "regexp"

// lineBreakChars are the Unicode line-break code points the reference
// server's channel-name predicate forbids anywhere in a name.
const lineBreakChars = "\n\v\f\r  "

// channelNameRegex mirrors the reference server's channel-name predicate
// (realtime lib/resource nameRegex): a name is one or more runes whose
// first rune is not ':' , ',', ASCII whitespace, or '[', and none of whose
// runes is a Unicode line break. The leading-'[' exclusion matches the
// reference's treatment of a bracket prefix as a scope/qualifier ([meta]
// etc.), which this server does not implement, so such names are rejected
// rather than parsed as qualified channels.
var channelNameRegex = regexp.MustCompile(`^[^:,\s\[][^` + lineBreakChars + `]*$`)

// ValidChannelName reports whether name is an acceptable channel name.
// An invalid name (empty, a reserved leading character, or a line break)
// is rejected with Ably error 40010 at ATTACH and at publish, on both the
// realtime and REST surfaces (DESIGN.md §4).
func ValidChannelName(name string) bool {
	return channelNameRegex.MatchString(name)
}

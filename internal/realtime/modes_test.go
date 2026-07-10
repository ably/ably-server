package realtime

import (
	"testing"

	"github.com/ably/ably-server/internal/protocol"
)

// TestResolveModesDefaultExcludesAnnotation pins the §4.2/§14.3 rule that a
// no-mode-bits ATTACH resolves to the four non-annotation modes only — the
// annotation modes are opt-in.
func TestResolveModesDefaultExcludesAnnotation(t *testing.T) {
	got := resolveModes(0)
	if got != defaultModes {
		t.Fatalf("resolveModes(0) = %b, want defaultModes %b", got, defaultModes)
	}
	if got&protocol.FlagAnnotationPublish != 0 || got&protocol.FlagAnnotationSubscribe != 0 {
		t.Errorf("default mode set includes an annotation mode: %b", got)
	}
}

// TestResolveModesPassesAnnotationOptIn ensures an ATTACH that explicitly
// requests an annotation mode has it recognised (not masked out).
func TestResolveModesPassesAnnotationOptIn(t *testing.T) {
	req := protocol.FlagSubscribe | protocol.FlagAnnotationSubscribe
	got := resolveModes(req)
	if got != req {
		t.Errorf("resolveModes(SUBSCRIBE|ANNOTATION_SUBSCRIBE) = %b, want %b", got, req)
	}
	pub := protocol.FlagAnnotationPublish
	if resolveModes(pub) != pub {
		t.Errorf("resolveModes(ANNOTATION_PUBLISH) = %b, want %b", resolveModes(pub), pub)
	}
}

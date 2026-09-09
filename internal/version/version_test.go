package version

import (
	"runtime"
	"strings"
	"testing"
)

// The stamped values are package-level, so a test that sets them must
// restore them for the next.
func stamp(t *testing.T, v, c string) {
	t.Helper()
	oldV, oldC := version, commit
	t.Cleanup(func() { version, commit = oldV, oldC })
	version, commit = v, c
}

func TestStringStamped(t *testing.T) {
	stamp(t, "v1.2.3", "deadbeefcafebabe1234567890abcdef12345678")
	got := String()
	want := "ably-server v1.2.3 (deadbeefcafe) " + runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// A stamped build reports the commit it was told about and does not
// claim the build container's tree was dirty.
func TestStampedIsNeverModified(t *testing.T) {
	stamp(t, "v1.2.3", "deadbeef")
	if Modified() {
		t.Error("Modified() = true for a stamped build, want false")
	}
	if got := Commit(); got != "deadbeef" {
		t.Errorf("Commit() = %q, want the stamped value", got)
	}
}

// An unstamped build says so rather than reporting the synthesised
// pseudo-version the toolchain derives from the commit.
func TestUnstampedVersionIsDev(t *testing.T) {
	stamp(t, "", "")
	if got := Version(); got != "dev" && !strings.HasPrefix(got, "v") {
		t.Errorf("Version() = %q, want %q or a module version", got, "dev")
	}
	if buildSetting("vcs.revision") != "" && Version() != "dev" {
		t.Errorf("Version() = %q for a VCS build, want %q", Version(), "dev")
	}
}

func TestStringAlwaysIdentifiesTheBinary(t *testing.T) {
	stamp(t, "", "")
	got := String()
	if !strings.HasPrefix(got, "ably-server ") {
		t.Errorf("String() = %q, want an ably-server prefix", got)
	}
	if !strings.Contains(got, runtime.GOARCH) {
		t.Errorf("String() = %q, want it to name the architecture", got)
	}
}

func TestShort(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"deadbeefcafebabe1234", "deadbeefcafe"},
		{"deadbeef", "deadbeef"},
		{"", ""},
	} {
		if got := short(tc.in); got != tc.want {
			t.Errorf("short(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

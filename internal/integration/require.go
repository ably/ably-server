package integration

import "testing"

// Require skips t unless the integration build tag is set. Integration
// tests call it as their first statement, so they compile in every build
// and run only when asked for (DESIGN.md §18).
func Require(t *testing.T) {
	t.Helper()
	if !Enabled {
		t.Skip("skipped: build with -tags=integration to run the integration tests")
	}
}

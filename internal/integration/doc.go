// Package integration reports whether the integration build tag is set.
//
// The integration tests themselves carry no build constraint, so they are
// compiled by an ordinary `go build` or `go vet` and a change that breaks
// one fails the build. What the tag decides is whether they *run*: each
// integration test calls Require first, which skips it unless Enabled
// (DESIGN.md §18). Guarding per test rather than per package means a
// package is free to hold unit and integration tests side by side.
package integration

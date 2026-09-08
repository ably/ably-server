package handles

import "github.com/ably/server-protocol/go/errors"

// This file is whether the app is served (DESIGN.md §9.1).
//
// On Ably an app carries a status from the account behind it, which changes
// for commercial reasons this server has none of — it has no account, no
// limits and no billing relationship, so nothing that would tell a restricted
// app apart from a blocked one. What it has instead is a file, so that the
// states a client has to cope with — being refused, and being cut off
// mid-connection — are states this server can be put into.
//
// So the file is a boolean wearing a word.

// StatusEnabled is the one thing the app-status file can say that leaves the
// app served. Anything else disables it: another word, a typo, a stray
// character. There is deliberately no vocabulary of statuses here, because
// this server would do the same thing for every one of them.
const StatusEnabled = "enabled"

// StatusDisabled is the word to write to disable the app. Nothing reads it —
// anything but StatusEnabled would do — but a file has to say something, and
// this is what it should say.
const StatusDisabled = "disabled"

// EnabledByStatus reads the app-status file's contents. An empty string is
// enabled, since the file is optional and a server with no file is a server
// with nothing wrong with it.
func EnabledByStatus(status string) bool {
	return status == "" || status == StatusEnabled
}

// errAppDisabled is what a client is told while the app is not served. It is
// one value, shared, because it never changes: there is only one way for this
// server's app to be unserviceable and only one thing to say about it.
var errAppDisabled = errors.New(40300, 403, "This application is disabled")

// This file is the part of the configuration that is read again while the
// server runs, rather than once before it starts (DESIGN.md §9).
//
// On Ably an app's keys, its namespaces and its status all change under a
// running server, and the protocol module is built to pick those changes up:
// a key reference keeps resolving, a channel watches its namespace, a
// connection watches for the app becoming unserviceable. A server whose whole
// configuration is fixed at startup exercises none of that. These sources
// give this one somewhere for a change to come from — a directory per
// collection, one file per entry, plus a file holding the app's status — so
// that editing a file here is this server's equivalent of an app being
// reconfigured on Ably.

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	protoapp "github.com/ably/server-protocol/go/app"
)

// Dynamic names the configuration sources that are re-read while the server
// runs. An empty path means that source is not watched at all.
type Dynamic struct {
	// KeysDir holds one file per API key, each a TOML document in the same
	// shape as a [[keys]] entry. The key's identity comes from the file's
	// contents, not its name, so a file may be called anything.
	KeysDir string

	// NamespacesDir holds one file per namespace, each a TOML document in the
	// same shape as a [[namespaces]] entry. As with keys, the name of the file
	// carries no meaning.
	NamespacesDir string

	// AppStatusFile holds the app's status as a bare string — one of the names
	// in Status. No file means the app is enabled, so that an operator who
	// never writes one gets a server that always serves.
	AppStatusFile string
}

// Watched reports whether anything is watched at all.
func (d Dynamic) Watched() bool {
	return d.KeysDir != "" || d.NamespacesDir != "" || d.AppStatusFile != ""
}

// Snapshot is one reading of the watched sources: everything they said at a
// single moment. Two snapshots comparing equal means nothing changed, which is
// how a poll decides it has nothing to apply.
type Snapshot struct {
	Keys       []KeyEntry
	Namespaces []Namespace
	AppStatus  string
}

// Equal reports whether two snapshots say the same thing.
func (s Snapshot) Equal(other Snapshot) bool {
	return s.AppStatus == other.AppStatus &&
		slices.Equal(s.Keys, other.Keys) &&
		slices.Equal(s.Namespaces, other.Namespaces)
}

// Read takes a snapshot of every watched source. A source that is not watched
// contributes nothing rather than being an error, so a server watching only
// one of the three reads only that one.
//
// A watched directory that does not exist is an error: it was named
// explicitly, and a typo that quietly configures nothing is worse than a
// refusal. A missing app-status file is not, since absence is how an enabled
// app is spelled.
func (d Dynamic) Read() (Snapshot, error) {
	var snap Snapshot

	if d.KeysDir != "" {
		keys, err := readKeysDir(d.KeysDir)
		if err != nil {
			return Snapshot{}, err
		}
		snap.Keys = keys
	}
	if d.NamespacesDir != "" {
		namespaces, err := readNamespacesDir(d.NamespacesDir)
		if err != nil {
			return Snapshot{}, err
		}
		snap.Namespaces = namespaces
	}
	if d.AppStatusFile != "" {
		status, err := readAppStatusFile(d.AppStatusFile)
		if err != nil {
			return Snapshot{}, err
		}
		snap.AppStatus = status
	}
	return snap, nil
}

// readKeysDir reads one [[keys]] entry per file in dir.
func readKeysDir(dir string) ([]KeyEntry, error) {
	paths, err := entryFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("config: keys directory: %w", err)
	}
	entries := make([]KeyEntry, 0, len(paths))
	for _, path := range paths {
		var entry KeyEntry
		if _, err := toml.DecodeFile(path, &entry); err != nil {
			return nil, fmt.Errorf("config: parse %q: %w", path, err)
		}
		if entry.Key == "" {
			return nil, fmt.Errorf("config: %q declares no key", path)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// readNamespacesDir reads one [[namespaces]] entry per file in dir.
//
// Each entry is stamped with its file's modification time, which is the version
// the namespace map compares (see Namespace.Modified): the file is the record
// of what the namespace says, so when it was last written is the record of
// which version of it this is.
func readNamespacesDir(dir string) ([]Namespace, error) {
	paths, err := entryFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("config: namespaces directory: %w", err)
	}
	entries := make([]Namespace, 0, len(paths))
	for _, path := range paths {
		var entry Namespace
		if _, err := toml.DecodeFile(path, &entry); err != nil {
			return nil, fmt.Errorf("config: parse %q: %w", path, err)
		}
		if entry.ID == "" {
			return nil, fmt.Errorf("config: %q declares no namespace id", path)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("config: %q: %w", path, err)
		}
		entry.Modified = info.ModTime().UnixMilli()
		entries = append(entries, entry)
	}
	return entries, nil
}

// StampNamespaces returns the namespaces with the given version, for the ones
// which do not come from a watched file: the config file's and the flags' are
// re-applied unchanged for as long as the server runs, so one version for the
// lot of them is the whole truth about them.
func StampNamespaces(namespaces []Namespace, modified time.Time) []Namespace {
	stamped := make([]Namespace, len(namespaces))
	for i, ns := range namespaces {
		ns.Modified = modified.UnixMilli()
		stamped[i] = ns
	}
	return stamped
}

// readAppStatusFile reads the app's status, which is the file's whole
// contents with the surrounding whitespace stripped — an operator writing one
// with `echo` should not have to think about the trailing newline. A missing
// file reads as the empty string, which callers take as enabled.
func readAppStatusFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("config: app status file: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// entryFiles lists the files in dir holding one entry each: every regular
// file whose name does not begin with a dot, in name order.
//
// Dotfiles are skipped because that is where an editor's swap file and a
// Kubernetes ConfigMap's internal ..data directory both live, and reading
// either as a config entry would be a parse error rather than a config
// change. os.ReadDir already sorts by name, so the order a snapshot reports
// does not depend on the filesystem.
func entryFiles(dir string) ([]string, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(dirEntries))
	for _, e := range dirEntries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	return paths, nil
}

// ValidateNamespaces checks the namespaces a server is about to apply,
// whether they came from the config file or from the watched directory. A
// namespace with no id, or a mode this server does not recognise, is refused.
//
// An unknown mode is refused rather than ignored because the protocol code
// reads any mode it does not recognise as the default one: `mode = "matchers"`
// would otherwise apply the rule to a different set of channels than the one
// written down.
func ValidateNamespaces(namespaces []Namespace) error {
	for i, ns := range namespaces {
		if ns.ID == "" {
			return fmt.Errorf("namespace #%d has no id", i)
		}
		if ns.Mode != "" && ns.Mode != protoapp.NamespaceModeMatcher {
			return fmt.Errorf("namespace %q has mode %q, want %q or none",
				ns.ID, ns.Mode, protoapp.NamespaceModeMatcher)
		}
	}
	return nil
}

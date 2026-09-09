package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFile writes one watched config file under dir.
func writeFile(t *testing.T, dir, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o644); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
}

func TestDynamicReadsEachSource(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	namespacesDir := filepath.Join(dir, "namespaces")
	statusFile := filepath.Join(dir, "app-status")
	for _, d := range []string{keysDir, namespacesDir} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Two keys, read in name order regardless of the order they were written.
	writeFile(t, keysDir, "b.toml", "key = \"app.two:s2\"\n")
	writeFile(t, keysDir, "a.toml", "key = \"app.one:s1\"\ncapability = '{\"chat:*\":[\"subscribe\"]}'\n")
	// A dotfile is an editor's business, not a key.
	writeFile(t, keysDir, ".swp", "nonsense")

	writeFile(t, namespacesDir, "persisted.toml", "id = \"persisted\"\npersisted = true\n")
	writeFile(t, dir, "app-status", "  disabled\n")

	sources := Dynamic{KeysDir: keysDir, NamespacesDir: namespacesDir, AppStatusFile: statusFile}
	if !sources.Watched() {
		t.Error("Watched() = false, want true")
	}

	got, err := sources.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := Snapshot{
		Keys: []KeyEntry{
			{Key: "app.one:s1", Capability: `{"chat:*":["subscribe"]}`},
			{Key: "app.two:s2"},
		},
		// A namespace carries its file's modification time, which is the
		// version the namespace map compares to decide what changed.
		Namespaces: []Namespace{{ID: "persisted", Persisted: true, Modified: modTime(t, filepath.Join(namespacesDir, "persisted.toml"))}},
		AppStatus:  "disabled",
	}
	if !got.Equal(want) {
		t.Errorf("Read() = %+v, want %+v", got, want)
	}

	// Reading again with nothing touched says the same thing, which is how a
	// poll decides it has no work.
	again, err := sources.Read()
	if err != nil {
		t.Fatalf("Read again: %v", err)
	}
	if !again.Equal(got) {
		t.Errorf("an unchanged read differs: %+v vs %+v", again, got)
	}

	// A changed key file is a changed snapshot.
	writeFile(t, keysDir, "a.toml", "key = \"app.one:s1\"\n")
	changed, err := sources.Read()
	if err != nil {
		t.Fatalf("Read after edit: %v", err)
	}
	if changed.Equal(got) {
		t.Error("a narrowed capability did not change the snapshot")
	}
}

// An unwatched source contributes nothing rather than being an error, so a
// server watching one of the three reads only that one.
func TestDynamicUnwatchedSourcesAreEmpty(t *testing.T) {
	var sources Dynamic
	if sources.Watched() {
		t.Error("Watched() = true for a zero Dynamic")
	}
	got, err := sources.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !got.Equal(Snapshot{}) {
		t.Errorf("Read() = %+v, want the zero snapshot", got)
	}
}

// No app-status file means enabled, which is the empty status: an operator
// who never writes one gets a server that always serves.
func TestDynamicMissingAppStatusFileIsEmpty(t *testing.T) {
	sources := Dynamic{AppStatusFile: filepath.Join(t.TempDir(), "nope")}
	got, err := sources.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.AppStatus != "" {
		t.Errorf("AppStatus = %q, want empty", got.AppStatus)
	}
}

func TestDynamicRejectsUnreadableSources(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	if err := os.Mkdir(keysDir, 0o755); err != nil {
		t.Fatal(err)
	}

	for name, sources := range map[string]Dynamic{
		// A directory named explicitly and not there is a typo, not an empty
		// configuration.
		"missing keys directory":       {KeysDir: filepath.Join(dir, "nope")},
		"missing namespaces directory": {NamespacesDir: filepath.Join(dir, "nope")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := sources.Read(); err == nil {
				t.Error("Read() = nil error, want error")
			}
		})
	}

	for name, contents := range map[string]string{
		"unparseable": "this is not toml",
		"no key":      "capability = '{\"*\":[\"*\"]}'\n",
	} {
		t.Run(name, func(t *testing.T) {
			writeFile(t, keysDir, "k.toml", contents)
			if _, err := (Dynamic{KeysDir: keysDir}).Read(); err == nil {
				t.Error("Read() = nil error, want error")
			}
		})
	}

	namespacesDir := filepath.Join(dir, "namespaces")
	if err := os.Mkdir(namespacesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, namespacesDir, "ns.toml", "persisted = true\n")
	if _, err := (Dynamic{NamespacesDir: namespacesDir}).Read(); err == nil {
		t.Error("a namespace with no id: Read() = nil error, want error")
	}
}

func TestValidateNamespaces(t *testing.T) {
	if err := ValidateNamespaces([]Namespace{
		{ID: "persisted", Persisted: true},
		{ID: "*:edits", Mode: "matcher"},
	}); err != nil {
		t.Errorf("ValidateNamespaces: %v", err)
	}

	for name, namespaces := range map[string][]Namespace{
		"no id": {{Persisted: true}},
		// A mode the protocol code does not recognise reads as the default
		// one, so a typo would quietly apply the rule to other channels.
		"unknown mode": {{ID: "ns", Mode: "matchers"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateNamespaces(namespaces); err == nil {
				t.Error("ValidateNamespaces = nil error, want error")
			}
		})
	}
}

// modTime is a file's modification time in the units Namespace.Modified is in.
func modTime(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.ModTime().UnixMilli()
}

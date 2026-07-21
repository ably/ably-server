package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

func TestRunCleanExitsZero(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "results.json")
	ignore := filepath.Join(dir, "ignore.toml")
	writeFile(t, results, `{"results":[{"name":"TestA","verdict":"pass","duration":0.1},{"name":"TestB","verdict":"fail","duration":0.1}]}`)
	writeFile(t, ignore, "[[failure]]\ntest = \"TestB\"\nreason = \"known gap\"\n")

	var stdout, stderr bytes.Buffer
	code := run([]string{"--results", results, "--ignore", ignore}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestRunRegressionExitsNonzero(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "results.json")
	ignore := filepath.Join(dir, "ignore.toml")
	writeFile(t, results, `{"results":[{"name":"TestA","verdict":"fail","duration":0.1}]}`)
	writeFile(t, ignore, "")

	var stdout, stderr bytes.Buffer
	code := run([]string{"--results", results, "--ignore", ignore}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit code = 0, want nonzero; stdout=%s", stdout.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("TestA")) {
		t.Errorf("stdout = %q, want it to name TestA", stdout.String())
	}
}

func TestRunStaleExitsNonzero(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "results.json")
	ignore := filepath.Join(dir, "ignore.toml")
	writeFile(t, results, `{"results":[{"name":"TestA","verdict":"pass","duration":0.1}]}`)
	writeFile(t, ignore, "[[failure]]\ntest = \"TestA\"\nreason = \"used to fail\"\n")

	var stdout, stderr bytes.Buffer
	code := run([]string{"--results", results, "--ignore", ignore}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit code = 0, want nonzero; stdout=%s", stdout.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("TestA")) {
		t.Errorf("stdout = %q, want it to name TestA", stdout.String())
	}
}

func TestRunMissingFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

func TestRunBadIgnoreListExitsNonzero(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "results.json")
	ignore := filepath.Join(dir, "ignore.toml")
	writeFile(t, results, `{"results":[]}`)
	writeFile(t, ignore, "[[failure]]\ntest = \"TestA\"\n") // missing reason

	var stdout, stderr bytes.Buffer
	code := run([]string{"--results", results, "--ignore", ignore}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr=%s", code, stderr.String())
	}
}

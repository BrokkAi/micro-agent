package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCmdShellQuoting(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "with space.txt"), []byte("quoted-ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	tr := &turn{s: &session{record: record{Cwd: dir}}}
	result := tr.shellLocal(context.Background(), resolveShell("cmd"), `type "with space.txt"`, 10*time.Second)
	if result.failed || !strings.Contains(result.output, "quoted-ok") {
		t.Fatalf("result = %+v", result)
	}
}

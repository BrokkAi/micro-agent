package agent

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// shellKind selects how a command string is passed to the shell.
type shellKind int

const (
	posixShell shellKind = iota
	powerShell
	cmdShell
)

type shell struct {
	path string
	kind shellKind
}

// resolveShell picks the configured shell, or the platform default:
// PowerShell (then cmd) on Windows, bash (then sh) elsewhere.
func resolveShell(configured string) shell {
	candidates := []string{configured}
	if configured == "" {
		if runtime.GOOS == "windows" {
			candidates = []string{"pwsh", "powershell", "cmd"}
		} else {
			candidates = []string{"bash", "sh"}
		}
	}
	for _, name := range candidates {
		if path, err := exec.LookPath(name); err == nil {
			return shell{path: path, kind: kindOf(path)}
		}
	}
	return shell{path: candidates[0], kind: kindOf(candidates[0])}
}

func kindOf(path string) shellKind {
	switch strings.TrimSuffix(strings.ToLower(filepath.Base(path)), ".exe") {
	case "pwsh", "powershell":
		return powerShell
	case "cmd":
		return cmdShell
	}
	return posixShell
}

// args builds the argument list that runs command.
func (s shell) args(command string) []string {
	switch s.kind {
	case powerShell:
		return []string{"-NoProfile", "-NonInteractive", "-Command", "try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}\n" + command}
	case cmdShell:
		return []string{"/d", "/s", "/c", command}
	}
	return []string{"-c", command}
}

// name is a human-readable label for prompts and tool descriptions.
func (s shell) name() string {
	base := strings.TrimSuffix(filepath.Base(s.path), ".exe")
	switch s.kind {
	case powerShell:
		return "PowerShell (" + base + ")"
	case cmdShell:
		return "cmd.exe"
	}
	return base
}

const (
	maxOutputBytes = 50 * 1024
	maxOutputLines = 2000
)

// limitOutput bounds text for the model. keepTail keeps the end (shell
// output, where errors and summaries land); otherwise the start is kept. When
// anything is dropped, the full text is saved to a temp file the model can
// read or search.
func limitOutput(text, label string, keepTail bool) string {
	lines := strings.SplitAfter(text, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(text) <= maxOutputBytes && len(lines) <= maxOutputLines {
		return text
	}
	var kept []string
	size := 0
	for i := range lines {
		index := i
		if keepTail {
			index = len(lines) - 1 - i
		}
		line := lines[index]
		if len(kept) == maxOutputLines || size+len(line) > maxOutputBytes {
			break
		}
		kept = append(kept, line)
		size += len(line)
	}
	first, last := 1, len(kept)
	if keepTail {
		for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
			kept[i], kept[j] = kept[j], kept[i]
		}
		first, last = len(lines)-len(kept)+1, len(lines)
	}
	body := strings.Join(kept, "")
	if len(kept) == 0 {
		// A single enormous line: show a byte slice of it.
		if keepTail {
			body = text[len(text)-maxOutputBytes:]
		} else {
			body = text[:maxOutputBytes]
		}
	}
	footer := fmt.Sprintf("[Showing lines %d-%d of %d (%s total).", first, last, len(lines), byteSize(len(text)))
	if path, err := saveOutput(text, label); err == nil {
		footer += " Full output: " + path
	}
	footer += "]"
	if keepTail {
		return footer + "\n" + body
	}
	return strings.TrimRight(body, "\n") + "\n" + footer
}

func saveOutput(text, label string) (string, error) {
	var b [8]byte
	_, _ = rand.Read(b[:])
	path := filepath.Join(os.TempDir(), "micro-agent-"+label+"-"+hex.EncodeToString(b[:])+".log")
	return path, os.WriteFile(path, []byte(text), 0o600)
}

func byteSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}

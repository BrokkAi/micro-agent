//go:build !windows

package agent

import "os/exec"

func prepareCommand(*exec.Cmd, shell, string) {}

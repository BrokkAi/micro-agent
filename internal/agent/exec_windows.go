package agent

import (
	"os/exec"
	"syscall"
)

// prepareCommand builds cmd.exe's command line by hand: Go's argument
// escaping (\" for inner quotes) is not understood by cmd.exe. With /s, cmd
// strips the outer quotes and runs the rest verbatim.
func prepareCommand(cmd *exec.Cmd, sh shell, command string) {
	if sh.kind == cmdShell {
		cmd.SysProcAttr = &syscall.SysProcAttr{CmdLine: `"` + sh.path + `" /d /s /c "` + command + `"`}
	}
}

//go:build linux

package main

import (
	"os/exec"
	"syscall"
)

// setGroup starts cmd in its own process group, so a kill takes the proxy
// and the server it launched, as when an agent host kills a tool process.
func setGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

func killGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

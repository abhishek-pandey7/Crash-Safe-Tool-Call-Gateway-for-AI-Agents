//go:build !linux

package main

import "os/exec"

// The comparison runs on Linux; elsewhere only the process itself is killed.
func setGroup(*exec.Cmd) {}

func killGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

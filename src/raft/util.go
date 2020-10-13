package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) int {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return 0
}

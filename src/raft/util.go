package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) int {
	log.SetFlags(log.Ldate | log.Lmicroseconds) //精确到毫秒，并且抬头信息为时间+文件名+源代码所在行号
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return 0
}

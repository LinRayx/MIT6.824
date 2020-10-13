package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

var MAPTASK int = 1
var REDUCETASK int = 2
var ALLTASKDONE int = 3
var MAPING int = 4
var REDUCING int = 5
var REGISTERED int = 6
var MAPFINISHED int = 7
var REDUCEFINISHED int = 8
var DUMP int = 9
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type WorkArgs struct {
	ReduceNumber int
	MapNumber int
	Intermediata []string
	WorkerId int
}

type WorkReply struct {
	Filename string
	MapNumber int
	ReduceNumber int
	ReduceFiles []string
	NReduce int
	Status int
	WorkerId int
	Inv int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

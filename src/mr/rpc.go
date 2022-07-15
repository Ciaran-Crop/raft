package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	Map TaskType = 1
	Reduce TaskType = 2
	Done TaskType = 3
)

type AskTaskArgs struct {

}

type AskTaskReply struct {
	TaskType TaskType

	// task number of either map or reduce task
	TaskNum int

	// map(to know how many reduce file to write)
	NReduceTasks int
	// map(to know which file to read)
	MapFile string

	//reduce(to know how many map files to read)
	NMapTasks int
}

type FinishTaskArgs struct {
	TaskType TaskType

	TaskNum int
}

type FinishTaskReply struct {

}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

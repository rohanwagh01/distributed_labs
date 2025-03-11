package mr

//
// RPC definitions. DONE
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Consts for tasks
type TaskType int

const (
	TaskTypeMap    TaskType = 0
	TaskTypeReduce TaskType = 1
	TaskTypeWait   TaskType = 3
	TaskTypeStop   TaskType = 4
)

//REQUEST WORK RPC

type RequestWorkArgs struct { //doesn't need to send anything to request work
}

type RequestWorkReply struct { //should not actually have to return anything to the worker i think?
	TypeOfTask TaskType
	PTask      *Task
	//ID         int    //Task ID number (can overlap with map or reduce tasks as stored in different taskArrays)
	//Location   string //Location where the task is either reading from (mapTask), might be "" for a reducetask just cause i will use internal documentation
	//Nfiles     int
}

//WORK COMPLETE RPC

type WorkCompleteArgs struct {
	CompletedTaskID int
	TypeOfTask      TaskType
}

type WorkCompleteReply struct { //should not actually have to return anything to the worker i think?
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

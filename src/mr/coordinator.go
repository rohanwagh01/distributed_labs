package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//CONSTS FOR INTERNAL REFERENCE AND STATE MANAGING

type State string

const (
	stateIdle       State = "IDLE"
	stateInProgress State = "INPROGRESS"
	stateComplete   State = "COMPLETE"
)

//Coordinator Struct Tree

type Coordinator struct {
	numMapTasks    int      // num Maps
	mapTaskList    TaskList // list of all the maps tasks
	numReduceTasks int      // num Reduce
	reduceTaskList TaskList // list of all the reduce tasks
}

type TaskList struct {
	AllTasks    []*Task
	numComplete int        // how many of the tasks have been completed, can update every time a task is switched to finished, then used to see if the map/reduce is done
	mu          sync.Mutex //lock as may have multiple threads working to check or assign stuff so it should be lockable
}

type Task struct { //may need to add time and concurrency stuff later (if only accessed while holding tasklist lock should be fine to leave without a lock itself)
	ID       int    //Task ID number (can overlap with map or reduce tasks as stored in different taskArrays)
	Location string //Location where the task is either reading from (mapTask), might be "" for a reducetask just cause i will use internal documentation
	STATE    State  //what state the task is in
	Nfiles   int    //nReduce for map tasks and nMaps for the reduce tasks (says how many files to make/look through)
}

// BUILD COORDINATOR FUNCTIONS (Not an RPC Handler)

func makeMapTaskList(files []string, nReduce int) *TaskList {
	mapTaskList := TaskList{
		AllTasks: make([]*Task, len(files)),
	}
	//for each filename, make a new task to add to the task list
	for i, filename := range files {
		mapTaskList.AllTasks[i] = &Task{
			ID:       i,
			Location: filename,
			STATE:    stateIdle,
			Nfiles:   nReduce,
		}
	}
	return &mapTaskList
}

func makeReduceTaskList(nReduce int, nMaps int) *TaskList {
	reduceTaskList := TaskList{
		AllTasks: make([]*Task, nReduce),
	}
	//for each filename, make a new task to add to the task list
	for i := 0; i < nReduce; i++ {
		reduceTaskList.AllTasks[i] = &Task{
			ID:     i,
			STATE:  stateIdle,
			Nfiles: nMaps,
		}
	}
	return &reduceTaskList
}

func checkOnTaskAfterTen(tl *TaskList, id int) {
	//wait ten seconds
	time.Sleep(10 * time.Second)

	tl.mu.Lock()
	defer tl.mu.Unlock()

	//make the job back to idle so something else can take it
	if tl.AllTasks[id].STATE == stateInProgress {
		tl.AllTasks[id].STATE = stateIdle
	}
}

// RPC HANDLERS

func (c *Coordinator) HandleRequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	//check if there is an open map task, assign if so, wait if not
	c.mapTaskList.mu.Lock()
	defer c.mapTaskList.mu.Unlock()
	if c.mapTaskList.numComplete < c.numMapTasks { //if there are remaining map tasks
		for i := range c.mapTaskList.AllTasks { //check if any are showing idle
			if c.mapTaskList.AllTasks[i].STATE == stateIdle { //can be asigned
				c.mapTaskList.AllTasks[i].STATE = stateInProgress
				reply.TypeOfTask = TaskTypeMap
				reply.PTask = c.mapTaskList.AllTasks[i]
				//reply.ID = c.mapTaskList.AllTasks[i].ID
				//reply.Location = c.mapTaskList.AllTasks[i].Location
				//reply.Nfiles = c.mapTaskList.AllTasks[i].Nfiles
				//fmt.Println("outgoing RPC", reply)
				go checkOnTaskAfterTen(&c.mapTaskList, i)
				return nil
			}
		}
		//there are still map tasks that are being run but none are idle ADD CHECK FOR REASSIGNMENT HERE
		//for i := range c.mapTaskList.AllTasks {
		//	if c.mapTaskList.AllTasks[i].STATE == stateInProgress && time.Since(c.mapTaskList.AllTasks[i].TaskStartTime) > 10*time.Second { //in progress and long, so reassign
		//		c.mapTaskList.AllTasks[i].TaskStartTime = time.Now()
		//		reply.TypeOfTask = TaskTypeMap
		//		reply.PTask = c.mapTaskList.AllTasks[i]
		//		return nil
		//	}
		//}
		reply.TypeOfTask = TaskTypeWait
		return nil
	}
	//check if there is an open reduce task, assign if so, wait if not
	c.reduceTaskList.mu.Lock()
	defer c.reduceTaskList.mu.Unlock()
	if c.reduceTaskList.numComplete < c.numReduceTasks { //if there are remaining reduce tasks
		for i := range c.reduceTaskList.AllTasks { //check if any are showing idle
			if c.reduceTaskList.AllTasks[i].STATE == stateIdle { //can be asigned
				c.reduceTaskList.AllTasks[i].STATE = stateInProgress
				reply.TypeOfTask = TaskTypeReduce
				reply.PTask = c.reduceTaskList.AllTasks[i]
				//reply.ID = c.reduceTaskList.AllTasks[i].ID
				//reply.Location = c.reduceTaskList.AllTasks[i].Location
				//reply.Nfiles = c.reduceTaskList.AllTasks[i].Nfiles
				go checkOnTaskAfterTen(&c.reduceTaskList, i)
				return nil
			}
		}
		//there are still map tasks that are being run but none are idle ADD CHECK FOR REASSIGNMENT HERE
		//for i := range c.reduceTaskList.AllTasks {
		//	if c.reduceTaskList.AllTasks[i].STATE == stateInProgress && time.Since(c.reduceTaskList.AllTasks[i].TaskStartTime) > 10*time.Second { //in progress and long, so reassign
		//		c.reduceTaskList.AllTasks[i].TaskStartTime = time.Now()
		//		reply.TypeOfTask = TaskTypeMap
		//		reply.PTask = c.reduceTaskList.AllTasks[i]
		//		return nil
		//	}
		//}
		//there are still reduce tasks that are being run but none are idle
		reply.TypeOfTask = TaskTypeWait
		return nil
	}
	//nothing open, then tell to stop
	reply.TypeOfTask = TaskTypeStop
	return nil
}

func (c *Coordinator) logCompletion(tl *TaskList, id int) error { //helper function to handle updating the task list when something is done
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if tl.AllTasks[id].STATE != stateComplete { //avoid double counting for two workers running the same process, possible lfor idle to go to complete if the job was over ten seconds
		tl.AllTasks[id].STATE = stateComplete
		tl.numComplete += 1
	}
	return nil
}

func (c *Coordinator) HandleWorkComplete(args *WorkCompleteArgs, reply *WorkCompleteReply) error {
	switch args.TypeOfTask {
	case TaskTypeMap:
		c.logCompletion(&c.mapTaskList, args.CompletedTaskID)
	case TaskTypeReduce:
		c.logCompletion(&c.reduceTaskList, args.CompletedTaskID)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool { //COMPLETE
	c.reduceTaskList.mu.Lock() //wait for lock to be available
	defer c.reduceTaskList.mu.Unlock()
	return c.reduceTaskList.numComplete == c.numReduceTasks //if the number of complete tasks is equal to the number of tasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator { //COMPLETE
	c := Coordinator{
		numMapTasks:    len(files), //one map task per input file (says in lab the partitions are by file)
		numReduceTasks: nReduce,
		//may need to add something to handle when tasks are finished?
	}

	// Make Task Lists for Map and Reduce
	c.mapTaskList = *makeMapTaskList(files, nReduce)
	c.reduceTaskList = *makeReduceTaskList(nReduce, len(files))

	//should be setup to now wait for workers to request assignments through RPC handlers

	c.server()
	return &c
}

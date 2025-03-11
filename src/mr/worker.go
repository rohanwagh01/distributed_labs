package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// HELPER FUNCTIONS FOR MAPPING AND REDUCING
// mostly copied from sequential
func applyMap(mapf func(string, string) []KeyValue, workToComplete *RequestWorkReply) {
	intermediate := []KeyValue{}
	filename := workToComplete.PTask.Location
	//open file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	//now have a key val list, need to put into nReduce num intermediate files for each word
	niFiles := workToComplete.PTask.Nfiles

	//each intermediate file needs to store key value pairs to add to the file
	iFilesContents := make([][]KeyValue, niFiles)

	//populate
	for _, kv := range intermediate {
		//hash and put in bucket
		hashIndex := ihash(kv.Key) % niFiles
		iFilesContents[hashIndex] = append(iFilesContents[hashIndex], kv)
	}

	//now all kv are stored, place in intermediate buckets
	for reduceTaskNum, contents := range iFilesContents {
		//put in temp file then copy
		tFile, err := os.CreateTemp("", "tempiFile-*")
		if err != nil {
			fmt.Println("Error creating temp file:", err)
			return
		}
		//do the json encoding
		enc := json.NewEncoder(tFile)
		for _, kv := range contents {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Error encoding kv to json:", err)
				return
			}
		}
		iFilename := fmt.Sprintf("mr-%v-%v", workToComplete.PTask.ID, reduceTaskNum)
		//rename atomically
		err = os.Rename(tFile.Name(), iFilename)
		if err != nil {
			fmt.Println("Error renaming file:", err)
			return
		}
		os.Remove(tFile.Name()) //close temporary file
	}
	//notify that map is complete
	CallWorkComplete(workToComplete.PTask.ID, TaskTypeMap)
}

// mostly copied from sequential
func applyReduce(reducef func(string, []string) string, workToComplete *RequestWorkReply) {
	//remake intermediate from all the intermediate json files
	intermediate := []KeyValue{}
	nmFiles := workToComplete.PTask.Nfiles //number of maps to look through
	//for each of the files, read and update intermediate
	for mapTaskNum := 0; mapTaskNum < nmFiles; mapTaskNum++ {
		iFilename := fmt.Sprintf("mr-%v-%v", mapTaskNum, workToComplete.PTask.ID)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("cannot open %v", iFilename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv) //add each kv to the intermediate
		}
	}

	sort.Sort(ByKey(intermediate))

	tFile, err := os.CreateTemp("", "tempoutFile-*")
	if err != nil {
		fmt.Println("Error creating temp file:", err)
		return
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	//replace with final output
	outFilename := fmt.Sprintf("mr-out-%v", workToComplete.PTask.ID)
	//rename atomically
	err = os.Rename(tFile.Name(), outFilename)
	if err != nil {
		fmt.Println("Error renaming file:", err)
		return
	}
	os.Remove(tFile.Name()) //close temporary file
	//notify that reduce is complete
	CallWorkComplete(workToComplete.PTask.ID, TaskTypeReduce)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for { //inf loop for worker processes
		workToComplete := CallRequestWork()
		//fmt.Println("incoming RPC: ", *workToComplete)
		//switch work response
		switch workToComplete.TypeOfTask {
		case TaskTypeMap:
			applyMap(mapf, workToComplete)
		case TaskTypeReduce:
			applyReduce(reducef, workToComplete)
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeStop: //stop the worker, leave the function
			return
		}
	}
}

//RPC Calls

func CallRequestWork() *RequestWorkReply {

	args := RequestWorkArgs{}

	reply := RequestWorkReply{}
	ok := call("Coordinator.HandleRequestWork", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Println("failed RPC")
		return &RequestWorkReply{} //return an empty if the call failed so that worker can skip
	}
}

func CallWorkComplete(id int, tt TaskType) {

	args := WorkCompleteArgs{}

	args.CompletedTaskID = id
	args.TypeOfTask = tt

	reply := WorkCompleteReply{}
	call("Coordinator.HandleWorkComplete", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

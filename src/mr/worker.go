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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	err := os.Rename(tmpFile, finalFile)
	if err != nil {
		panic(err)
	}
}

func getIntermediateFile(mapTaskN int, reduceTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, reduceTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, reduceTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, reduceTaskN)
	err := os.Rename(tmpFile, finalFile)
	if err != nil {
		panic(err)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		call("Coordinator.HandleGetTask", &args, &reply)
		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task Type? %v\n", reply.TaskType)
		}

		finishedArgs := FinishTaskArgs{}
		finishedReply := FinishTaskReply{}
		finishedArgs.TaskType = reply.TaskType
		finishedArgs.TaskNum = reply.TaskNum
		//fmt.Printf("fininshTask %v-%v\n",reply.TaskType,reply.TaskNum)
		call("Coordinator.HandleFinishedTask", &finishedArgs, &finishedReply)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func performReduce(Tasknum int, NMaptasks int, reducef func(string, []string) string) {
	// get all intermediate files and collect
	kva := []KeyValue{}
	for n := 0; n < NMaptasks; n++ {
		iFilename := getIntermediateFile(n, Tasknum)
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
			kva = append(kva, kv)
		}
		file.Close()
	}
	// sort
	sort.Sort(ByKey(kva))

	// get tmp reduce file to write values
	tmpFile, err := ioutil.TempFile(".", "")
	if err != nil {
		log.Fatalf("cannot open tmpfile")
	}
	tmpFilename := tmpFile.Name()
	key_begin := 0
	for key_begin < len(kva) {
		key_end := key_begin + 1
		for key_end < len(kva) && kva[key_begin].Key == kva[key_end].Key {
			key_end++
		}
		values := []string{}
		for i := key_begin; i < key_end; i++ {
			values = append(values, kva[i].Value)
		}
		output := reducef(kva[key_begin].Key, values)
		//fmt.Printf("key: %v, value: %v", kva[key_begin].Key,output)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)

		key_begin = key_end
	}
	finalizeReduceFile(tmpFilename, Tasknum)
}

func performMap(filename string, taskNum int, NReduceTasks int, mapf func(string, string) []KeyValue) {
	// read contents to map
	//fmt.Printf("filename :%v, taskNum:%v, NReduceTasks:%v\n", filename,taskNum,NReduceTasks)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// apply map function to contents of file and collect
	kva := mapf(filename, string(content))
	//fmt.Println(kva)
	// create tmp files and encoders for each file
	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < NReduceTasks; r++ {
		tmpFile, err := ioutil.TempFile(".", "")
		if err != nil {
			log.Fatalf("cannot open tmpfile")
		}
		//fmt.Printf("tmpfilename %v\n",tmpFile.Name())
		tmpFiles = append(tmpFiles, tmpFile)
		name := tmpFile.Name()
		tmpFilenames = append(tmpFilenames, name)
		encoder := json.NewEncoder(tmpFile)
		encoders = append(encoders, encoder)
	}

	// write output keys to tmp intermediate files
	for _, kv := range kva {
		r := ihash(kv.Key) % NReduceTasks
		encoders[r].Encode(&kv)
	}
	// close file
	for _, f := range tmpFiles {
		f.Close()
	}
	// change filename
	for r := 0; r < NReduceTasks; r++ {
		finalizeIntermediateFile(tmpFilenames[r], taskNum, r)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

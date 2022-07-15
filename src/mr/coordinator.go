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

type Coordinator struct {
	// Your definitions here.

	// lock
	mu sync.Mutex

	cond *sync.Cond

	MapFiles     []string
	nMapTasks    int
	nReduceTasks int

	mapTasksFinished []bool
	mapTasksIssued   []time.Time

	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HandleGetTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	// issue map tasks until there are no map tasks left
	for {
		mapDone := true
		// range all map task list to get state
		for n, done := range c.mapTasksFinished {
			// if this map task is not done
			if !done {
				if c.mapTasksIssued[n].IsZero() ||
					time.Since(c.mapTasksIssued[n]).Seconds() > 25 {
					reply.TaskType = Map
					reply.TaskNum = n
					reply.MapFile = c.MapFiles[n]
					c.mapTasksIssued[n] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}
		// if all maps are in progress and haven't timed out,wait to give another task
		if !mapDone {
			c.cond.Wait()
		} else {
			break
		}
	}
	// all map tasks are done, issue reduce tasks until there are no reduce tasks left
	for {
		reduceDone := true
		for n, done := range c.reduceTasksFinished {
			if !done {
				if c.reduceTasksIssued[n].IsZero() ||
					time.Since(c.reduceTasksIssued[n]).Seconds() > 25 {
					reply.TaskType = Reduce
					reply.TaskNum = n
					c.reduceTasksIssued[n] = time.Now()
					return nil
				} else {
					reduceDone = false
				}
			}
		}
		if !reduceDone {
			c.cond.Wait()
		} else {
			break
		}
	}
	reply.TaskType = Done
	c.isDone = true
	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case Map:
		c.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad finished task? %v", args.TaskType)
	}
	c.cond.Broadcast()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.isDone
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.cond = sync.NewCond(&c.mu)
	c.MapFiles = files
	c.nMapTasks = len(files)
	c.nReduceTasks = nReduce
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	// create a goroutine to wake up the GetTask handler thread every once in awhile to check if some task hasn't
	// finished, so we can know to reissue it
	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second * 25)
		}
	}()

	c.server()
	return &c
}

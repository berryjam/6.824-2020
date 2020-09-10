package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mutex sync.RWMutex

var finishedMutex sync.RWMutex

var mapTaskMutex sync.Mutex

var reduceTaskMutex sync.Mutex

type TaskStatus int

type Phase int

type WorkerTaskStatus int

const (
	NotYetStarted TaskStatus = iota
	Doing
	Done
)

const (
	Success WorkerTaskStatus = iota
	Fail
)

const (
	MapPhase Phase = iota
	ReducePhase
)

type Master struct {
	// Your definitions here.
	Files               []string
	MapNum              int
	ReduceNum           int
	MapTaskStatusMap    []TaskStatus
	ReduceTaskStatusMap []TaskStatus
	CurPhase            Phase
	Finished            bool
	DoneMapNum          int
	DoneReduceNum       int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	mutex.Lock()
	fmt.Printf("master status:%+v\n", *m)
	if m.Finished {
		reply.JobDone = true
		return nil
	}
	reply.JobDone = false
	if m.CurPhase == MapPhase {
		mapTaskMutex.Lock()
		for i := 0; i < len(m.MapTaskStatusMap); i++ {
			if m.MapTaskStatusMap[i] == NotYetStarted {
				reply.MapFile = m.Files[i]
				reply.CurPhase = MapPhase
				reply.MapNum = m.MapNum
				reply.ReduceNum = m.ReduceNum
				reply.MapTaskIdx = i
				m.MapTaskStatusMap[i] = Doing
				go func(mapTaskIdx int) {
					timer := time.NewTimer(time.Second * 10)
					<-timer.C
					if m.MapTaskStatusMap[mapTaskIdx] == Doing {
						m.MapTaskStatusMap[mapTaskIdx] = NotYetStarted
					}
				}(i)
				break
			}
		}
		mapTaskMutex.Unlock()
	} else if m.CurPhase == ReducePhase {
		reduceTaskMutex.Lock()
		for i := 0; i < len(m.ReduceTaskStatusMap); i++ {
			if m.ReduceTaskStatusMap[i] == NotYetStarted {
				reply.CurPhase = ReducePhase
				reply.MapNum = m.MapNum
				reply.ReduceNum = m.ReduceNum
				reply.ReduceTaskIdx = i
				m.ReduceTaskStatusMap[i] = Doing
				go func(reduceTaskIdx int) {
					timer := time.NewTimer(time.Second * 10)
					<-timer.C
					reduceTaskMutex.Lock()
					if m.ReduceTaskStatusMap[reduceTaskIdx] == Doing {
						m.ReduceTaskStatusMap[reduceTaskIdx] = NotYetStarted
					}
				}(i)
				break
			}
		}
		reduceTaskMutex.Unlock()
	}
	mutex.Unlock()

	return nil
}

func (m *Master) NotifyWorkerTaskStatus(args *NotifyWorkerTaskStatusArgs, reply *NotifyWorkerTaskStatusReply) error {
	mutex.Lock()
	if args.WorkerPhase == MapPhase {
		mapTaskMutex.Lock()
		if args.Status == Success {
			m.MapTaskStatusMap[args.TaskIdx] = Done
			m.DoneMapNum++
			if m.DoneMapNum == m.MapNum {
				m.CurPhase = ReducePhase
			}
		} else if args.Status == Fail {
			m.MapTaskStatusMap[args.TaskIdx] = NotYetStarted
		} else {
			return fmt.Errorf("unknown task status:%+v phase:%+v taskIdx:%+v", args.Status, args.WorkerPhase, args.TaskIdx)
		}
		mapTaskMutex.Unlock()
	} else if args.WorkerPhase == ReducePhase {
		reduceTaskMutex.Lock()
		if args.Status == Success {
			m.ReduceTaskStatusMap[args.TaskIdx] = Done
			m.DoneReduceNum++
			if m.DoneReduceNum == m.ReduceNum {
				finishedMutex.Lock()
				m.Finished = true
				finishedMutex.Unlock()
			}
		} else if args.Status == Fail {
			m.ReduceTaskStatusMap[args.TaskIdx] = NotYetStarted
		} else {
			return fmt.Errorf("unknown task status:%+v phase:%+v taskIdx:%+v", args.Status, args.WorkerPhase, args.TaskIdx)
		}
		reduceTaskMutex.Unlock()
	}
	mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	finishedMutex.RLock()
	ret = m.Finished
	finishedMutex.RUnlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.Files = files
	m.MapNum = len(files)
	m.ReduceNum = nReduce
	m.DoneMapNum = 0
	m.DoneReduceNum = 0
	m.Finished = false
	m.MapTaskStatusMap = make([]TaskStatus, len(files))
	for i := 0; i < len(m.Files); i++ {
		m.MapTaskStatusMap[i] = NotYetStarted
	}
	m.ReduceTaskStatusMap = make([]TaskStatus, nReduce)
	for i := 0; i < nReduce; i++ {
		m.ReduceTaskStatusMap[i] = NotYetStarted
	}
	m.CurPhase = MapPhase

	m.server()
	return &m
}

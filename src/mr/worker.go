package mr

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	var intermediateEncoderMap map[int]*json.Encoder = nil
	var intermediateFileNameMap map[int]string = nil
	for {
		askForReply := AskForTask()
		if askForReply.JobDone {
			break
		}
		switch askForReply.CurPhase {
		case MapPhase:
			if intermediateEncoderMap == nil {
				uuidInstance := uuid.New()
				intermediateEncoderMap = make(map[int]*json.Encoder)
				intermediateFileNameMap = make(map[int]string)
				for j := 0; j < askForReply.ReduceNum; j++ {
					fileName := fmt.Sprintf("mr-tmp-%+v-%+v-%+v", uuidInstance.String(), askForReply.MapTaskIdx, j)
					file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Fatalf("cannot open intermediateEncoderFile:%v mapIdx:%+v reduceIdx:%+v", fileName, askForReply.MapTaskIdx, j)
					}
					intermediateEncoderMap[j] = json.NewEncoder(file)
					intermediateFileNameMap[j] = fileName
				}
			}

			fileName := askForReply.MapFile
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open mapFile:%v", fileName)
				break
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
				break
			}
			file.Close()
			kva := mapf(fileName, string(content))
			mapPhaseSucc := true
			for i := 0; i < len(kva); i++ {
				reduceTaskIdx := ihash(kva[i].Key) % askForReply.ReduceNum
				intermediateEncoder := intermediateEncoderMap[reduceTaskIdx]
				if err := intermediateEncoder.Encode(kva[i]); err != nil {
					log.Fatalf("encode kv:%v failed", kva[i])
					mapPhaseSucc = false
					NotifyWorkerTaskStatus(MapPhase, askForReply.MapTaskIdx, Fail)
					intermediateEncoderMap = nil
					intermediateFileNameMap = nil
					break
				}
			}
			if mapPhaseSucc {
				for reduceIdx, fileName := range intermediateFileNameMap {
					os.Rename(fileName, fmt.Sprintf("mr-%+v-%+v", askForReply.MapTaskIdx, reduceIdx))
				}
				intermediateEncoderMap = nil
				intermediateFileNameMap = nil
				NotifyWorkerTaskStatus(MapPhase, askForReply.MapTaskIdx, Success)
			}
		case ReducePhase:
			intermediate := []KeyValue{}
			for i := 0; i < askForReply.MapNum; i++ {
				fileName := fmt.Sprintf("mr-%+v-%+v", i, askForReply.ReduceTaskIdx)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open intermediateDecoderFile:%v mapIdx:%+v reduceIdx:%+v", fileName, i, askForReply.ReduceTaskIdx)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%+v", askForReply.ReduceTaskIdx)
			uuidInstance := uuid.New()
			uuidFileName := fmt.Sprintf("mr-tmp-reduce-%v", uuidInstance.String())
			ofile, err := os.OpenFile(uuidFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("cannot open uuidFileName:%v", uuidFileName)
				break
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(uuidFileName, oname)
			NotifyWorkerTaskStatus(ReducePhase, askForReply.ReduceTaskIdx, Success)
			for i := 0; i < askForReply.MapNum; i++ {
				fileName := fmt.Sprintf("mr-%+v-%+v", i, askForReply.ReduceTaskIdx)
				err := os.Remove(fileName)
				if err != nil {
					log.Fatalf("cannot remove tmp file:%v", fileName)
				}
			}
			ofile.Close()
		}

		time.Sleep(time.Second)
	}
}

func AskForTask() AskForTaskReply {
	args := AskForTaskArgs{}

	reply := AskForTaskReply{}

	call("Master.AskForTask", &args, &reply)

	return reply
}

func NotifyWorkerTaskStatus(phase Phase, taskIdx int, status WorkerTaskStatus) {
	args := NotifyWorkerTaskStatusArgs{}
	args.WorkerPhase = phase
	args.TaskIdx = taskIdx
	args.Status = status
	reply := NotifyWorkerTaskStatusReply{}

	call("Master.NotifyWorkerTaskStatus", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

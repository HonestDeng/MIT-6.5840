package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := requestTask()
		switch task.taskType {
		case "END":
			os.Exit(0)
		case "WAIT":
			time.Sleep(time.Second)
		case "MAP":
			file, err := os.Open(task.files[0])
			if err != nil {
				// TODO: 处理err
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				// TODO: 处理err
			}
			file.Close()

			intermediate := mapf(task.files[0], string(content))

			// 创建nReduce个文件
			ofilenames := make([]string, task.nReduce)
			ofiles := make([]*os.File, task.nReduce)
			encs := make([]*json.Encoder, task.nReduce)
			for i := 0; i < task.nReduce; i++ {
				filename := "mr-" + strconv.Itoa(task.taskID) + "-" + strconv.Itoa(i)
				ofilenames[i] = filename
				f, err := os.Create(filename)
				if err != nil {
					// TODO: 处理err
				}
				ofiles[i] = f
				encs[i] = json.NewEncoder(f)
			}
			for _, kv := range intermediate {
				i := ihash(kv.Key)
				err := encs[i].Encode(kv.Value)
				if err != nil {
					// TODO: 处理err
				}
			}
			// 关闭文件
			for _, file := range ofiles {
				err := file.Close()
				if err != nil {
					// TODO: 处理err
				}
			}

			// 将结果文件提交给coordinator
			args, reply := HandInResultArgs{}, HandInResultReply{}
			args.taskID, args.taskType, args.resultFiles = task.taskID, task.taskType, ofilenames
			handInResult(&args, &reply)

		case "REDUCE":
			intermediate := []KeyValue{}
			for _, file_name := range task.files {
				file, err := os.Open(file_name)
				if err != nil {
					// TODO: 处理异常
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

			oname := "mr-out-" + strconv.Itoa(task.taskID)
			ofile, _ := os.Create(oname)

			// shuffle然后调用reduce
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
				output := reducef(intermediate[i].Key, values) // 调用reduce

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()

			// 将结果文件提交给coordinator
			ofilename := make([]string, 1)
			ofilename[0] = oname
			args, reply := HandInResultArgs{}, HandInResultReply{}
			args.taskID, args.taskType, args.resultFiles = task.taskID, task.taskType, ofilename
			handInResult(&args, &reply)
		}
	}
}

func requestTask() TaskReply {
	args := TaskRequest{}
	reply := TaskReply{}
	ok := call("Coordinator.assignTask", &args, &reply)
	if !ok {
		// TODO: 处理异常
	}
	return reply
}

func handInResult(args *HandInResultArgs, reply *HandInResultReply) {
	ok := call("Coordinator.handInResult", args, reply)
	if !ok {
		// TODO: 处理异常
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

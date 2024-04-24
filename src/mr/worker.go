package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
)
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := requestTask()
		switch task.TaskType {
		case "END":
			log.Println("Get END task.")
			os.Exit(0)
		case "WAIT":
			log.Println("Get WAIT task. Sleep for a second.")
			time.Sleep(time.Second)
		case "MAP":
			log.Println("Get MAP task with task id ", task.TaskID, ".")
			log.Println("Read file", task.Files[0])
			file, err := os.Open(task.Files[0])
			if err != nil {
				log.Printf("Fail to open file: %v", task.Files[0])
				log.Println(err)
				os.Exit(0)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Printf("Fail to read file: %v", task.Files[0])
				log.Println(err)
				os.Exit(0)
			}
			file.Close()

			intermediate := mapf(task.Files[0], string(content))

			// 创建nReduce个文件
			ofilenames := make([]string, task.NReduce)
			ofiles := make([]*os.File, task.NReduce)
			encs := make([]*json.Encoder, task.NReduce)
			for i := 0; i < task.NReduce; i++ {
				filename := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i) + ".txt"
				log.Println("Create file", filename)
				ofilenames[i] = filename
				f, err := os.Create(filename)
				if err != nil {
					log.Printf("Fail to create file: %v", filename)
					log.Println(err)
					os.Exit(0)
				}
				ofiles[i] = f
				encs[i] = json.NewEncoder(f)
			}
			for _, kv := range intermediate {
				i := ihash(kv.Key) % task.NReduce
				err := encs[i].Encode(&kv)
				if err != nil {
					log.Printf("Fail to write into file: %v, %v", ofilenames[i], kv)
					log.Println(err)
					os.Exit(0)
				}
			}
			// 关闭文件
			for i, file := range ofiles {
				err := file.Close()
				if err != nil {
					log.Printf("Fail to close file: %v, %v", ofilenames[i])
					log.Println(err)
					os.Exit(0)
				}
			}

			// 将结果文件提交给coordinator
			args, reply := HandInResultArgs{}, HandInResultReply{}
			args.TaskID, args.TaskType, args.ResultFiles = task.TaskID, task.TaskType, ofilenames
			handInResult(&args, &reply)

		case "REDUCE":
			log.Println("Get REDUCE task with task id ", task.TaskID, ".")
			intermediate := []KeyValue{}
			for _, file_name := range task.Files {
				file, err := os.Open(file_name)
				if err != nil {
					log.Printf("Fail to open file: %v\n", file_name)
					log.Println(err)
					os.Exit(0)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						// 读取完毕
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(task.TaskID)
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
			args.TaskID, args.TaskType, args.ResultFiles = task.TaskID, task.TaskType, ofilename
			handInResult(&args, &reply)
		}
	}
}

func requestTask() TaskReply {
	log.Println("Request task from Coordinator.")
	args := TaskRequest{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		// TODO: 处理异常
		log.Printf("requestTask: Cannot connect Coordinator. Maybe job is Done. It's time to exit.", args)
		os.Exit(0)
	}
	return reply
}

func handInResult(args *HandInResultArgs, reply *HandInResultReply) {
	log.Println("Hand in completed job to Coordinator.")
	ok := call("Coordinator.HandInResult", args, reply)
	if !ok {
		// TODO: 处理异常
		log.Printf("handInResult: Cannot connect Coordinator. Maybe job is Done. It't time to exit.", args)
		os.Exit(0)
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
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

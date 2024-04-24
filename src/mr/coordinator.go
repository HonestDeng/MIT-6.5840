package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "container/heap"

// =================== 最小堆用于存储时间戳 ===================
type pair [2]int64 // pair = [timestamp, id]
type MinHeap []pair

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Less(i, j int) bool {
	return h[i][0] < h[j][0]
}

func (h *MinHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(pair))
}

func (h *MinHeap) Pop() interface{} {
	res := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return res
}

// ============= Coordinator ==============
type Coordinator struct {
	// Your definitions here.
	failureTime       int64
	inputFile         []string   // 待处理的文件
	intermediateFiles [][]string // 中间文件。第一维是taskID，第二维是nReduce
	nReduce           int        // reduce的任务数
	mutex             sync.Mutex

	waitingMapTasks  map[string]int // 未完成的Map任务。key是待处理的文件名，value是taskID
	solvingMapTasks  map[string]int // 正在进行中的任务
	finishedMapTasks map[string]int // 已经完成的Map任务
	mapHeap          MinHeap

	waitingReduceTasks  map[int]int // 未完成的Reduce任务。key是taskID，value没有使用
	solvingReduceTasks  map[int]int // 正在进行中的任务
	finishedReduceTasks map[int]int // 已经完成的Reduce任务
	reduceHeap          MinHeap
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	// 使用锁保护共享资源
	c.mutex.Lock()
	log.Println("* Current Map task statues:")
	log.Println(c.waitingMapTasks)
	log.Println(c.solvingMapTasks)
	log.Println(c.finishedMapTasks)
	log.Println(c.mapHeap)
	log.Println("* Current Reduce task status: ")
	log.Println(c.waitingReduceTasks)
	log.Println(c.solvingReduceTasks)
	log.Println(c.finishedReduceTasks)
	log.Println(c.reduceHeap)

	// 先将超时任务从solving状态转移到waiting状态
	for c.mapHeap.Len() > 0 && time.Now().UnixMicro()-c.mapHeap[0][0] >= c.failureTime {
		taskID := c.mapHeap[0][1]
		key := c.inputFile[taskID]
		c.waitingMapTasks[key] = int(taskID)
		delete(c.solvingMapTasks, key)

		log.Println("Due to time out, move map task ", taskID, "from solving to waiting. Time is", time.Now().UnixMicro())

		heap.Pop(&c.mapHeap)
	}
	for c.reduceHeap.Len() > 0 && time.Now().UnixMicro()-c.reduceHeap[0][0] >= c.failureTime {
		taskID := c.reduceHeap[0][1]
		c.waitingReduceTasks[int(taskID)] = 1
		delete(c.solvingReduceTasks, int(taskID))

		log.Println("Due to time out, move reduce task ", taskID, "from solving to waiting")

		heap.Pop(&c.reduceHeap)
	}

	log.Println("* Current Map task statues:")
	log.Println(c.waitingMapTasks)
	log.Println(c.solvingMapTasks)
	log.Println(c.finishedMapTasks)
	log.Println(c.mapHeap)
	log.Println("* Current Reduce task status: ")
	log.Println(c.waitingReduceTasks)
	log.Println(c.solvingReduceTasks)
	log.Println(c.finishedReduceTasks)
	log.Println(c.reduceHeap)

	// 分派任务：如果还有未完成的map，则不会分派reduce任务
	if len(c.waitingMapTasks) > 0 { // 如果仍然有map任务没有分配
		log.Println("* There are ", len(c.waitingMapTasks), " map task waiting to be assigned")
		filenames := make([]string, 1)
		var taskID int
		for k := range c.waitingMapTasks {
			filenames[0] = k
			taskID = c.waitingMapTasks[k]
			break
		}

		c.solvingMapTasks[filenames[0]] = taskID // 将任务状态迁移到solving
		delete(c.waitingMapTasks, filenames[0])

		reply.Files = filenames
		reply.TaskType = "MAP"
		reply.NReduce = c.nReduce
		reply.TaskID = taskID

		// 记录任务执行的开始时间
		pii := pair{time.Now().UnixMicro(), int64(taskID)}
		heap.Push(&c.mapHeap, pii)

		log.Println("Assign map task with taskID ", taskID)
	} else if len(c.finishedMapTasks) == len(c.inputFile) && len(c.waitingReduceTasks) > 0 { // 尝试分配reduce任务
		// 所有map任务已经完成，并且仍然有reduce任务没有分配，那么分配reduce任务
		log.Println("* There are ", len(c.waitingReduceTasks), " reduce task waiting to be assigned")
		filenames := make([]string, len(c.intermediateFiles))
		var taskID int
		for k := range c.waitingReduceTasks {
			taskID = k
			break
		}
		for i := 0; i < len(c.intermediateFiles); i++ {
			filenames[i] = c.intermediateFiles[i][taskID] // len(inputFile) x nReduce
		}

		c.solvingReduceTasks[taskID] = 1
		delete(c.waitingReduceTasks, taskID)

		reply.Files = filenames
		reply.TaskType = "REDUCE"
		reply.NReduce = c.nReduce
		reply.TaskID = taskID

		// 记录任务执行的开始时间
		pii := pair{time.Now().UnixMicro(), int64(taskID)}
		heap.Push(&c.reduceHeap, pii)

		log.Println("Assign reduce task with taskID ", taskID)
	} else if len(c.finishedReduceTasks) == c.nReduce {
		// 所有工作都已经完成了
		log.Println("All task Done.")
		reply.TaskType = "END"
	} else {
		// 暂时没有任务可以分派，通知worker进入等待状态
		log.Println("There is no waiting task to be assigned. Just wait a while.")
		reply.TaskType = "WAIT"
	}
	c.mutex.Unlock()

	return nil
}

func (c *Coordinator) HandInResult(args *HandInResultArgs, reply *HandInResultReply) error {
	// 使用锁来保护共享资源
	c.mutex.Lock()
	// TODO：万一是被认为是僵尸的worker提交result应该怎么处理？
	if args.TaskType == "MAP" {
		log.Println("Receive a completed MAP task with taskID ", args.TaskID)
		key := c.inputFile[args.TaskID]
		// 保存中间文件并创建reduce任务
		c.intermediateFiles[args.TaskID] = args.ResultFiles
		// 将task从solving状态转移到finished状态
		c.finishedMapTasks[key] = args.TaskID
		delete(c.solvingMapTasks, key)
		// 删除heap中的对应数据
		for i := 0; i < c.mapHeap.Len(); i++ {
			if c.mapHeap[i][1] == int64(args.TaskID) {
				heap.Remove(&c.mapHeap, i)
				break
			}
		}
	} else if args.TaskType == "REDUCE" {
		log.Println("Receive a completed REDUCE task with taskID ", args.TaskID)
		key := args.TaskID

		// 将task从solving状态转移到finished状态
		c.finishedReduceTasks[key] = 1
		delete(c.solvingReduceTasks, key)
		// 删除heap中的对应数据
		for i := 0; i < c.reduceHeap.Len(); i++ {
			if c.reduceHeap[i][1] == int64(args.TaskID) {
				heap.Remove(&c.reduceHeap, i)
				break
			}
		}
	}
	c.mutex.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.finishedReduceTasks) == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFile, c.nReduce, c.failureTime = files, nReduce, 10*1000000 // 10秒 = 10 * 10^6 微秒
	c.waitingMapTasks = make(map[string]int)
	c.solvingMapTasks = make(map[string]int)
	c.finishedMapTasks = make(map[string]int)
	c.waitingReduceTasks = make(map[int]int)
	c.solvingReduceTasks = make(map[int]int)
	c.finishedReduceTasks = make(map[int]int)
	c.intermediateFiles = make([][]string, len(c.inputFile)) // len(inputFile) x nReduce

	for i, file := range files {
		c.waitingMapTasks[file] = i
		c.intermediateFiles[i] = make([]string, c.nReduce)
	}
	for i := 0; i < c.nReduce; i++ {
		c.waitingReduceTasks[i] = 1 // 创建reduce任务
	}
	c.mapHeap, c.reduceHeap = MinHeap{}, MinHeap{}
	heap.Init(&c.mapHeap)
	heap.Init(&c.reduceHeap)

	c.server()
	return &c
}

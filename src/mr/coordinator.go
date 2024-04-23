package mr

import (
	"log"
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
	*h = append(*h, x.([2]int64))
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
func (c *Coordinator) assignTask(args *TaskRequest, reply *TaskReply) {
	// TODO: 使用锁保护共享资源
	// 先将超时任务从solving状态转移到waiting状态
	for c.mapHeap.Len() > 0 && time.Now().UnixNano()-c.mapHeap[0][0] >= c.failureTime {
		taskID := c.mapHeap[0][1]
		key := c.inputFile[taskID]
		c.waitingMapTasks[key] = int(taskID)
		delete(c.solvingMapTasks, key)

		heap.Pop(&c.mapHeap)
	}
	for c.reduceHeap.Len() > 0 && time.Now().UnixNano()-c.reduceHeap[0][0] >= c.failureTime {
		taskID := c.reduceHeap[0][1]
		c.waitingReduceTasks[int(taskID)] = 1
		delete(c.solvingReduceTasks, int(taskID))

		heap.Pop(&c.reduceHeap)
	}

	// 分派任务：如果还有未完成的map，则不会分派reduce任务
	if len(c.waitingMapTasks) > 0 { // 如果仍然有map任务没有分配
		filenames := make([]string, 1)
		var taskID int
		for k := range c.waitingMapTasks {
			filenames[0] = k
			taskID = c.waitingMapTasks[k]
			break
		}

		c.solvingMapTasks[filenames[0]] = taskID // 将任务状态迁移到solving
		delete(c.waitingMapTasks, filenames[0])

		reply.files = filenames
		reply.taskType = "MAP"
		reply.nReduce = c.nReduce
		reply.taskID = taskID

		// 记录任务执行的开始时间
		pii := pair{time.Now().UnixNano(), int64(taskID)}
		heap.Push(&c.mapHeap, pii)
	} else if len(c.finishedMapTasks) == len(c.inputFile) && len(c.waitingReduceTasks) > 0 { // 尝试分配reduce任务
		// 所有map任务已经完成，并且仍然有reduce任务没有分配，那么分配reduce任务
		filenames := make([]string, c.nReduce)
		var taskID int
		for k := range c.waitingReduceTasks {
			filenames = c.intermediateFiles[k]
			taskID = k
			break
		}

		c.solvingReduceTasks[taskID] = 1
		delete(c.solvingReduceTasks, taskID)

		reply.files = filenames
		reply.taskType = "MAP"
		reply.nReduce = c.nReduce
		reply.taskID = taskID

		// 记录任务执行的开始时间
		pii := pair{time.Now().UnixNano(), int64(taskID)}
		heap.Push(&c.reduceHeap, pii)
	} else if len(c.finishedReduceTasks) == c.nReduce {
		// 所有工作都已经完成了
		reply.taskType = "END"
	} else {
		// 暂时没有任务可以分派，通知worker进入等待状态
		reply.taskType = "WAIT"
	}
}

func (c *Coordinator) handInResult(args *HandInResultArgs, reply *HandInResultReply) {
	// TODO: 使用锁来保护共享资源
	if args.taskType == "MAP" {
		key := c.inputFile[args.taskID]
		// 保存中间文件并创建reduce任务
		c.intermediateFiles[args.taskID] = args.resultFiles
		// 将task从solving状态转移到finished状态
		c.finishedMapTasks[key] = args.taskID
		delete(c.solvingMapTasks, key)
		// 删除heap中的对应数据
		for i := 0; i < c.mapHeap.Len(); i++ {
			if c.mapHeap[i][1] == int64(args.taskID) {
				heap.Remove(&c.mapHeap, i)
				break
			}
		}
	} else if args.taskType == "REDUCE" {
		key := args.taskID

		// 将task从solving状态转移到finished状态
		c.finishedReduceTasks[key] = 1
		delete(c.solvingReduceTasks, key)
		// 删除heap中的对应数据
		for i := 0; i < c.reduceHeap.Len(); i++ {
			if c.reduceHeap[i][1] == int64(args.taskID) {
				heap.Remove(&c.reduceHeap, i)
				break
			}
		}
	}
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
	return len(c.finishedReduceTasks) == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFile, c.nReduce, c.failureTime = files, nReduce, 10*1000 // 10秒 = 10 * 1000 毫秒
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

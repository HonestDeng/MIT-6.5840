package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	inputFile           []string       // 待处理的文件
	intermediateFiles   [][]string     // 中间文件。第一维是taskID，第二维是nReduce
	nReduce             int            // reduce的任务数
	waitingMapTasks     map[string]int // 未完成的Map任务。key是待处理的文件名，value是taskID
	finishedMapTasks    map[string]int // 已经完成的Map任务
	waitingReduceTasks  map[int]int    // 未完成的Reduce任务。key是taskID，value没有使用
	finishedReduceTasks map[int]int    // 已经完成的Reduce任务
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) assignTask(args *TaskRequest, reply *TaskReply) {
	// TODO: 使用锁保护共享资源
	if len(c.waitingMapTasks) > 0 {
		filenames := make([]string, 1)
		var taskID int
		for k := range c.waitingMapTasks {
			filenames[0] = k
			taskID = c.waitingMapTasks[k]
			break
		}
		c.finishedMapTasks[filenames[0]] = taskID
		delete(c.waitingMapTasks, filenames[0])

		reply.files = filenames
		reply.taskType = "MAP"
		reply.nReduce = c.nReduce
		reply.taskID = taskID
	} else if len(c.waitingReduceTasks) > 0 {
		filenames := make([]string, c.nReduce)
		var taskID int
		for k := range c.waitingReduceTasks {
			filenames = c.intermediateFiles[k]
			taskID = k
			break
		}
		c.finishedReduceTasks[taskID] = 1
		delete(c.waitingReduceTasks, taskID)

		reply.files = filenames
		reply.taskType = "MAP"
		reply.nReduce = c.nReduce
		reply.taskID = taskID
	} else if len(c.finishedReduceTasks) == c.nReduce {
		// 所有工作都已经完成了
		reply.taskType = "END"
	} else {
		// map任务才刚开始，还没有人map worker提交intermediate文件
		reply.taskType = "WAIT"
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
	for _, is_done := range c.mapTask {
		if !is_done {
			return false
		}
	}
	// TODO: fix bug, 有可能还没来得及将reduce任务放入到reduceTask中
	for _, is_done := range c.reduceTask {
		if !is_done {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFile, c.nReduce = files, nReduce
	for i, file := range files {
		c.waitingMapTasks[file] = i
		c.intermediateFiles[i] = make([]string, c.nReduce)
	}

	c.server()
	return &c
}

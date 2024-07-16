package mr

import "log"
import "net"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"
import "os"

type MapTask struct {
	id int
	filename string
}

type ReduceTask struct {
	id int
}

type Coordinator struct {
	mapTaskList []*MapTask
	reduceTaskList []*ReduceTask

	mapTaskReadyQueue []int
	mapTaskRunningSet map[int]bool

	reduceTaskReadyQueue []int
	reduceTaskRunningSet map[int]bool

	nReduce int

	mu sync.Mutex
}


func (c *Coordinator) AllocTask(args *AllocTaskArgs, reply *AllocTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce // 不太优雅

	if len(c.mapTaskReadyQueue) > 0 {
		mapTaskId := c.mapTaskReadyQueue[0]
		
		mapTask := c.mapTaskList[mapTaskId]
		c.mapTaskReadyQueue = c.mapTaskReadyQueue[1:]

		c.mapTaskRunningSet[mapTaskId] = true
		
		reply.TaskType = 1 // mapTask
		reply.TaskId = mapTaskId
		reply.Filename = mapTask.filename

		go c.checkMapRunningTask(mapTaskId)
		// log.Println("Coordinator:", "分配 mapTask", mapTaskId)
		return nil
	}
	if len(c.mapTaskRunningSet) == 0 && len(c.reduceTaskReadyQueue) > 0 {
		// map任务全部结束后，才开始reduce任务的分配
		reduceTaskId := c.reduceTaskReadyQueue[0]

		c.reduceTaskReadyQueue = c.reduceTaskReadyQueue[1:]
		c.reduceTaskRunningSet[reduceTaskId] = true

		reply.TaskType = 2 // reduceTask
		reply.TaskId = reduceTaskId
		go c.checkReduceRunningTask(reduceTaskId)
		// log.Println("Coordinator:", "分配 reduceTask", reduceTaskId)
		return nil
	}
	// 没有任务需要做了
	// log.Println("Coordinator:", "无任务需要分配")
	reply.TaskType = 0
	return nil
}

// should run in goroutine
func (c *Coordinator) checkMapRunningTask(mapTaskId int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTaskRunningSet[mapTaskId] {
		// log.Println("Coordinator:", "mapTask", mapTaskId, "超时")
		c.mapTaskRunningSet[mapTaskId] = false
		c.mapTaskReadyQueue = append(c.mapTaskReadyQueue, mapTaskId)
	}
}
// should run in goroutine
func (c *Coordinator) checkReduceRunningTask(reduceTaskId int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceTaskRunningSet[reduceTaskId] {
		// log.Println("Coordinator:", "reduceTask", reduceTaskId, "超时")
		c.reduceTaskRunningSet[reduceTaskId] = false
		c.reduceTaskReadyQueue = append(c.reduceTaskReadyQueue, reduceTaskId)
	}
}

func (c *Coordinator) CallBackMapTaskDone(args *CallBackMapTaskArgs, reply *CallBackMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.mapTaskRunningSet, args.TaskId)
	return nil
}
func (c *Coordinator) CallBackReduceTaskDone(args *CallBackReduceTaskArgs, reply *CallBackReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.reduceTaskRunningSet, args.TaskId)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.mapTaskReadyQueue) == 0 && len(c.mapTaskRunningSet) == 0 && len(c.reduceTaskReadyQueue) == 0 && len(c.reduceTaskRunningSet) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mapTaskList = []*MapTask{}
	c.reduceTaskList = []*ReduceTask{}
	c.mapTaskReadyQueue = []int{}
	c.mapTaskRunningSet = make(map[int]bool)
	c.reduceTaskReadyQueue = []int{}
	c.reduceTaskRunningSet = make(map[int]bool)
	c.nReduce = nReduce

	for i, filename := range files {
		mapTask := &MapTask{}
		mapTask.id = i
		mapTask.filename = filename
		c.mapTaskList = append(c.mapTaskList, mapTask)
		c.mapTaskReadyQueue = append(c.mapTaskReadyQueue, i)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := &ReduceTask{}
		reduceTask.id = i
		c.reduceTaskList = append(c.reduceTaskList, reduceTask)
		c.reduceTaskReadyQueue = append(c.reduceTaskReadyQueue, i)
	}

	for i := 0; i < nReduce; i++ {
		for j := 0; j < len(files); j++ {
			bucketName := fmt.Sprintf("mr-%v-%v", j, i)
			_, err := os.Create(bucketName)
			if err != nil {
				log.Fatalf(err.Error())
			}
		}
	}

	c.server()
	return &c
}

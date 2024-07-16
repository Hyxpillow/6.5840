package mr

import "log"
import "os"
import "net/rpc"
import "io/ioutil"
import "hash/fnv"
import "encoding/json"
import "sort"
import "fmt"
import "path/filepath"

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


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := AllocTaskArgs{}
	reply := AllocTaskReply{}
	done := call("Coordinator.AllocTask", &args, &reply)
	for !done {
		switch reply.TaskType {
		case 0: // do nothing
			// log.Println("worker:", "空闲...")
		case 1: // map
			// log.Println("worker:", "接收Map任务", "ID:", reply.TaskId, "文件名:", reply.Filename)
			filename := reply.Filename
			taskId := reply.TaskId
			nReduce := reply.NReduce
			
			// 读取文件进行map操作
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			//把中间kv存储进文件
			bucketList := []*json.Encoder{}
			for i := 0; i < nReduce; i++ {
				bucketName := fmt.Sprintf("mr-%v-%v", taskId, i)
				bucketFile, err := os.OpenFile(bucketName, os.O_WRONLY, os.ModeAppend)
				if err != nil {
					log.Fatalf(err.Error())
				}
				defer bucketFile.Close()
				enc := json.NewEncoder(bucketFile)
				bucketList = append(bucketList, enc)
			}

			for _, kv := range kva {
				i := ihash(kv.Key) % nReduce
				err := bucketList[i].Encode(&kv)
				if err != nil {
					log.Fatalf(err.Error())
				}
			}

			callBackArgs := CallBackMapTaskArgs{}
			callBackReply := CallBackMapTaskReply{}
			callBackArgs.TaskId = taskId
			call("Coordinator.CallBackMapTaskDone", &callBackArgs, &callBackReply)
		case 2: // reduce
			// log.Println("worker:", "接收Reduce任务", "ID:", reply.TaskId)
			reduceTaskId := reply.TaskId
			kva := []KeyValue{}

			filenames, _ := ioutil.ReadDir(".")
			for _, filename := range filenames {
				pattern := fmt.Sprintf("mr-*-%v", reduceTaskId)
				if match, _ := filepath.Match(pattern, filename.Name()); !match {
					continue
				}
				file, _ := os.Open(filename.Name())
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
			sort.Slice(kva, func (i, j int) bool {return kva[i].Key < kva[j].Key})

			oname := fmt.Sprintf("mr-out-%v", reduceTaskId)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()


			callBackArgs := CallBackReduceTaskArgs{}
			callBackReply := CallBackReduceTaskReply{}
			callBackArgs.TaskId = reduceTaskId
			call("Coordinator.CallBackReduceTaskDone", &callBackArgs, &callBackReply)
		}
		args = AllocTaskArgs{}
		reply = AllocTaskReply{}
		done = call("Coordinator.AllocTask", &args, &reply)
	}
	// log.Println("worker:", "所有任务已结束")
}


func call(rpcname string, args interface{}, reply interface{}) (done bool) {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return true
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err != nil {
		return true
	}
	return false
}

package mr

import (
	"encoding/json"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapStruct 		mapStruct
	reduceStruct	reduceStruct
}

type mapStruct struct {
	// key: fileName
	// val: "undone" "assigned" "done"
	taskState 	map[string]string
	taskId		map[string]int
	// undone task count
	undone 		int
	mu 			sync.Mutex
}

type reduceStruct struct {
	nReduce				int
	// 所有的interKey, 当作set用
	interKeys			map[string]bool
	// interKeysBucket[i] 为第i个reduce任务所包含的interKey
	// 一共nReduce个任务
	interKeysBucket		[][]string
	// key: taskIndex, 对应uReduce个任务
	// val: "undone" "assigned" "done"
	taskState 			map[int]string
	// undone task count
	undone 				int
	mu 					sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) RpcGetTask(args *RpcGetArgs, reply *RpcGetReply) error {
	m.mapStruct.mu.Lock()
	if m.mapStruct.undone != 0 {
		// map undone
		reply.TaskType = "wait"
		reply.NReduce = m.reduceStruct.nReduce
		for fileName := range m.mapStruct.taskState {
			if m.mapStruct.taskState[fileName] == "undone" {
				reply.FileName = fileName
				reply.TaskId = m.mapStruct.taskId[fileName]
				reply.TaskType = "map"
				m.mapStruct.taskState[fileName] = "assigned"
				break
			}
		}
	}else {
		// map done
		reply.TaskType = "reduce"
	}
	m.mapStruct.mu.Unlock()
	return nil
}

func (m *Master) RpcTaskDone(args *RpcPostArgs, reply *RpcPostReply) error {
	taskType := args.TaskType

	if taskType == "map" {
		// submit a map task
		m.mapStruct.mu.Lock()
		fileName := args.FileName
		if m.mapStruct.taskState[fileName] == "assigned" {
			m.mapStruct.taskState[fileName] = "done"
			m.mapStruct.undone--
			log.Printf("[Master] mapState: fileName: %v done!! undone = %v \n", fileName, m.mapStruct.undone)
			log.Printf("[Master] TaskMap: %v \n", m.mapStruct.taskState)

		}else {
			log.Printf("[Master] Error! A unassigned MapTask submit??? fileName = %v \n", fileName)
		}
		m.mapStruct.mu.Unlock()
	}
	return nil
}

func (m *Master) updateInterKeys(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("[Woker]: cannot open %v", fileName)
	}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		if m.reduceStruct.interKeys[kv.Key] == false {
			m.reduceStruct.interKeys[kv.Key] = true
		}
	}
}

func (m *Master) divideInterKeys()  {

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)			// 在RPC中注册master的方法
	rpc.HandleHTTP()		// 在Server中生成一个HTTP Handler，用于接收RPC消息
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Init m.mapStruct
	m.mapStruct.taskId = make(map[string]int)
	m.mapStruct.taskState = make(map[string]string)
	for i, fileName := range files {
		m.mapStruct.taskId[fileName] = i
		m.mapStruct.taskState[fileName] = "undone"
	}
	m.mapStruct.undone = len(files)
	log.Printf("[Master] init TaskMap: %v \n", m.mapStruct.taskState)

	// Init m.reduceStruct
	m.reduceStruct.nReduce = nReduce
	m.reduceStruct.interKeys = make(map[string]bool)
	m.reduceStruct.interKeysBucket = make([][]string, nReduce)
	m.reduceStruct.taskState = make(map[int]string)
	m.reduceStruct.undone = nReduce

	// start rpc server
	m.server()
	return &m
}

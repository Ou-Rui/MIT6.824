package mr

import (
	"log"
	"runtime"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapStruct 			mapStruct
	reduceStruct 		reduceStruct
	Wg           		sync.WaitGroup
	allDone				bool
}

type mapStruct struct {
	// key: fileName
	// val: "undone" "assigned" "done"
	taskState 	map[string]string
	taskId		map[string]int
	// undone task count
	done 		int
	mu 			sync.Mutex
}

type reduceStruct struct {
	nReduce				int
	// key: taskIndex, 对应uReduce个任务
	// val: "undone" "assigned" "done"
	taskState 			map[int]string
	// undone task count
	done 				int
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
	m.reduceStruct.mu.Lock()
	reply.TaskType = "wait"
	if m.mapStruct.done != len(m.mapStruct.taskState) {
		// assign map task
		reply.NReduce = m.reduceStruct.nReduce
		for fileName := range m.mapStruct.taskState {
			if m.mapStruct.taskState[fileName] == "undone" {
				reply.FileName = fileName
				reply.TaskId = m.mapStruct.taskId[fileName]
				reply.TaskType = "map"
				m.mapStruct.taskState[fileName] = "assigned"
				//m.Wg.Add(1)
				//go checkTask(m,"map", fileName, 0)
				break
			}
		}
	}else if m.reduceStruct.done != m.reduceStruct.nReduce {
		// assign reduce task
		for reduceId := range m.reduceStruct.taskState {
			if m.reduceStruct.taskState[reduceId] == "undone" {
				reply.TaskId = reduceId
				reply.NReduce = m.reduceStruct.nReduce
				reply.TaskType = "reduce"
				m.reduceStruct.taskState[reduceId] = "assigned"
				//m.Wg.Add(1)
				//go checkTask(m, "reduce", "", reduceId)
				break
			}
		}
	}
	m.reduceStruct.mu.Unlock()
	m.mapStruct.mu.Unlock()
	return nil
}

func (m *Master) RpcTaskDone(args *RpcPostArgs, reply *RpcPostReply) error {
	taskType := args.TaskType

	if taskType == "map" {
		// post a map task
		m.mapStruct.mu.Lock()
		fileName := args.FileName
		if m.mapStruct.taskState[fileName] == "assigned" {
			m.mapStruct.taskState[fileName] = "done"
			m.mapStruct.done++
			log.Printf("[Master] Map: fileName: %v done!! done = %v \n", fileName, m.mapStruct.done)
			log.Printf("[Master] Map TaskMap: %v \n", m.mapStruct.taskState)
		}else {
			log.Printf("[Master] Error! A unassigned MapTask submit??? fileName = %v \n", fileName)
		}
		m.mapStruct.mu.Unlock()
	}else if taskType == "reduce"{
		// post a reduce task
		m.reduceStruct.mu.Lock()
		reduceId := args.ReduceId
		if m.reduceStruct.taskState[reduceId] == "assigned" {
			m.reduceStruct.taskState[reduceId] = "done"
			m.reduceStruct.done++
			log.Printf("[Master] Reduce: id: %v done!! done = %v \n", reduceId, m.reduceStruct.done)
			log.Printf("[Master] Reduce TaskMap: %v \n", m.reduceStruct.taskState)
		}else {
			log.Printf("[Master] Error! A unassigned ReduceTask submit??? reduceId = %v \n", reduceId)
		}
		m.reduceStruct.mu.Unlock()
	}
	return nil
}


func checkTask(m *Master, taskType string, fileName string, reduceId int) {
	time.Sleep(20*time.Second)

	if taskType == "map" {
		m.mapStruct.mu.Lock()
		if m.mapStruct.taskState[fileName] == "assigned" {
			m.mapStruct.taskState[fileName] = "undone"
			log.Printf("[Master] Map Task Crash? fileName = %v", fileName)
			log.Printf("[Master] Map TaskMap: %v \n", m.mapStruct.taskState)
		}
		m.mapStruct.mu.Unlock()
	}else if taskType == "reduce" {
		m.reduceStruct.mu.Lock()
		if m.reduceStruct.taskState[reduceId] == "assigned" {
			m.reduceStruct.taskState[reduceId] = "undone"
			time.Sleep(time.Second)
			log.Printf("[Master] Reduce Task Crash? reduceId = %v", reduceId)
			log.Printf("[Master] Reduce TaskMap: %v \n", m.reduceStruct.taskState)
		}
		m.reduceStruct.mu.Unlock()
	}
	m.Wg.Done()
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
	m.reduceStruct.mu.Lock()
	if m.reduceStruct.done == m.reduceStruct.nReduce {
		m.allDone = true
		ret = true
		m.Wg.Wait()
	}
	m.reduceStruct.mu.Unlock()
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
	m.mapStruct.done = 0
	log.Printf("[Master] init TaskMap: %v \n", m.mapStruct.taskState)

	// Init m.reduceStruct
	m.reduceStruct.nReduce = nReduce
	m.reduceStruct.taskState = make(map[int]string)
	for i := 0; i < nReduce; i++ {
		m.reduceStruct.taskState[i] = "undone"
	}
	m.reduceStruct.done = 0

	// start rpc server
	m.server()
	//go m.looper()
	return &m
}

func (m *Master) looper()  {
	for {
		time.Sleep(10*time.Second)
		flag := true
		m.mapStruct.mu.Lock()
		m.reduceStruct.mu.Lock()
		for key := range m.mapStruct.taskState {
			if m.mapStruct.taskState[key] != "done" {
				flag = false
			}
			if m.mapStruct.taskState[key] == "assigned" {
				m.mapStruct.taskState[key] = "undone"
				log.Printf("[Master] Map Task Crash? fileName = %v", key)
				log.Printf("[Master] Map TaskMap: %v \n", m.mapStruct.taskState)
			}
		}
		for key := range m.reduceStruct.taskState {
			if m.reduceStruct.taskState[key] != "done" {
				flag = false
			}
			if m.reduceStruct.taskState[key] == "assigned" {
				m.reduceStruct.taskState[key] = "undone"
				log.Printf("[Master] Reduce Task Crash? reduceId = %v", key)
				log.Printf("[Master] Reduce TaskMap: %v \n", m.reduceStruct.taskState)
			}
		}
		m.mapStruct.mu.Unlock()
		m.reduceStruct.mu.Unlock()
		if flag {
			log.Printf("[Master] Looper Exit \n")
			runtime.Goexit()
		}
	}

}
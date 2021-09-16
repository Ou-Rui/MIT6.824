package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
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
	h := fnv.New32a()			// 返回一个空的Hash对象
	h.Write([]byte(key))		// 将key添加到Hash对象中
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for  {			// while true
		// RPC获取任务，返回
		reply := callGetTask()
		if reply.TaskType == "map" {
			log.Printf("[Worker] Get Map Task! FileName = %v \n", reply.FileName)
			workerMap(mapf,reply)
		}else if reply.TaskType == "reduce"{
			log.Printf("[Worker] Get Reduce Task!")
			time.Sleep(1*time.Second)
			break
		}else if reply.TaskType == "wait" {
			log.Printf("[Worker] Get Wait Task..zzzz")
			time.Sleep(1*time.Second)
			break
		}

	}


}

func workerMap(mapf func(string, string) []KeyValue, reply *RpcGetReply) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("[Woker]: cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker]: cannot read %v", reply.FileName)
	}
	kva := mapf(reply.FileName, string(content))	// map!

	var encoders = make(map[string]*json.Encoder)

	for _, kv := range kva {
		interFileName := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(ihash(kv.Key) % reply.NReduce) + ".txt"
		if encoders[interFileName] == nil {
			interFile, err := os.Create(interFileName)
			if err != nil {
				log.Fatalf("[Worker]: Open InterFile Error %v", err)
			}
			encoders[interFileName] = json.NewEncoder(interFile)
		}
		err = encoders[interFileName].Encode(&kv)
		if err != nil {
			log.Fatalf("[Worker]: InterFile Encode Error %v", err)
		}
	}
	callTaskDone("map", reply.FileName)
}


func callGetTask() *RpcGetReply {
	args := RpcGetArgs{}
	reply := RpcGetReply{}
	call("Master.RpcGetTask", &args, &reply)
	return &reply
}

func callTaskDone(takeType string, fileName string) {
	args := RpcPostArgs{TaskType: takeType, FileName: fileName}
	reply := RpcPostReply{}
	call("Master.RpcTaskDone", &args, &reply)
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



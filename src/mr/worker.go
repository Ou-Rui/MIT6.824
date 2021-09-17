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

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			workerMap(mapf, reply)
		}else if reply.TaskType == "reduce"{
			log.Printf("[Worker] Get Reduce Task! ReduceId = %v \n", reply.TaskId)
			workerReduce(reducef, reply)
		}else if reply.TaskType == "wait" {
			log.Printf("[Worker] Get wait Task...zzzz")
			time.Sleep(2*time.Second)
		}else {
			log.Printf("[Worker] Get None Task...exit")
			return
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
	callMapTaskDone(reply.FileName)
}

func workerReduce(reducef func(string, []string) string, reply *RpcGetReply) {
	reduceId := reply.TaskId
	outFileName := "mr-out-" + strconv.Itoa(reduceId) + ".txt"
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("[Worker]: Create OutputFile Error: %v", outFileName)
	}

	var interKVs []KeyValue
	// 一个ReduceTask, 需要对8个文件进行处理
	for i := 0; i <= 7; {
		interFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId) + ".txt"
		interFile, err := os.Open(interFileName)
		if err != nil {
			log.Printf("[Worker] Warning! Not Such InterFile %v", interFileName)
			i++
			continue
		}
		dec := json.NewDecoder(interFile)
		// 读取全部kv
		for  {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			interKVs = append(interKVs, kv)
		}
		i++
	}
	sort.Sort(ByKey(interKVs))
	log.Printf("[Worker] Reduce: interKVs sorted, id = %v, len = %v", reduceId, len(interKVs))
	for i := 0; i < len(interKVs); {
		var values []string
		var j int
		for j = i; j < len(interKVs) && interKVs[j].Key == interKVs[i].Key; j++ {
			values = append(values, interKVs[j].Value)
		}
		outputValue := reducef(interKVs[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", interKVs[i].Key, outputValue)
		i = j
	}
	callReduceTaskDone(reduceId)
}

func callGetTask() *RpcGetReply {
	args := RpcGetArgs{}
	reply := RpcGetReply{}
	call("Master.RpcGetTask", &args, &reply)
	return &reply
}

func callMapTaskDone(fileName string) {
	args := RpcPostArgs{TaskType: "map", FileName: fileName}
	reply := RpcPostReply{}
	call("Master.RpcTaskDone", &args, &reply)
}

func callReduceTaskDone(reduceId int) {
	args := RpcPostArgs{TaskType: "reduce", ReduceId: reduceId}
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



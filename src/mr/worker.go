package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//

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

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	// fmt.Printf("Woker inititialized ....\n")
	// uncomment to send the Example RPC to the coordinator.
	// WorkerJob = new(Job)
	// CallExample()

	for {

		// 得到 Task
		job := Job{}
		args := ExampleArgs{} // 这个参数没有用，返回的在 task中
		ok := call("Coordinator.DistributeTask", &args, &job)
		if !ok {
			log.Fatalf("DistributeTask failed")
		}

		if job.Jobtype == Waiting_task {
			// 休眠 1 秒钟
			// fmt.Printf("The task has been distributed but some are not finished\n")
			time.Sleep(1 * time.Second)
			continue
		}
		// 做完任务一直取
		// GetTaskFromCoordinator()
		// 执行任务
		ProcessTask(&job, mapf, reducef)

	}

}

var JobtypeToJobname = map[int]string{
	0: "MapTask",
	1: "ReduceTask",
	2: "WaitingTask",
}

func ProcessTask(job *Job, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// fmt.Printf("This is a [%s] task, Jobid is [%d].\n", JobtypeToJobname[job.Jobtype], job.Jobid)
	if job.Jobtype == Map_task {
		ProcessMapTask(job, mapf)
		// 做完之后发送一条信息给coordinator,希望把它阻塞队列中的Map任务给抹掉
		// SendFinishMessageToCoordinator()
		SendFinishMessageToCoordinator(job)
	} else if job.Jobtype == Reduce_task {
		ProcessReduceTask(job, reducef)
		// 做完之后发送一条信息给coordinator,希望把它阻塞队列中的Reduce任务给抹掉
		SendFinishMessageToCoordinator(job)
	}
}

func SendFinishMessageToCoordinator(job *Job) {
	// fmt.Printf("I finished a [%s] task, Jobid is [%d].\n", JobtypeToJobname[job.Jobtype], job.Jobid)
	call("Coordinator.ReceiveFinishMessage", job, nil)
}

func ProcessMapTask(job *Job, mapf func(string, string) []KeyValue) {
	// 参考mrsequential.go的代码
	filepath := job.Filename
	file, err := os.Open(filepath)
	// intermediate 是 .txt 文件所有单词的统计
	// 形式如 a:1 b:1 c:1 a:1 d:1 e:1 f:1
	intermediate := []KeyValue{}
	// fileInfo, err := file.Stat()
	// fmt.Printf("Open the file: [%s] file size is [%d]\n", filepath, fileInfo.Size())

	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	file.Close()

	kva := mapf(filepath, string(content))
	intermediate = append(intermediate, kva...)

	oname := "mr-out-" + strconv.Itoa(job.Jobid)
	for i := 0; i < job.NReduce; i++ {
		os.Remove(oname + "-" + strconv.Itoa(i))
	}

	kvas := make([][]KeyValue, job.NReduce)
	for i := 0; i < job.NReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}

	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, job.NReduce)
		kvas[index] = append(kvas[index], kv)
	}

	for i := 0; i < job.NReduce; i++ {
		sort.Slice(kvas[i], func(j, k int) bool {
			return kvas[i][j].Key < kvas[i][k].Key
		})
	}

	for i := 0; i < job.NReduce; i++ {
		// kvas[i] 写入文件
		filetemp, errtemp := os.Create(oname + "-" + strconv.Itoa(i))

		if errtemp != nil {
			log.Fatalf("cannot open %v", oname+"-"+strconv.Itoa(i))
		}

		// fmt.Printf("Successfully open the file: %s\n", oname + "-" + strconv.Itoa(i))

		enc := json.NewEncoder(filetemp)
		for _, kv := range kvas[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}

		filetemp.Close()
	}

}

func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}

func ProcessReduceTask(job *Job, reducef func(string, []string) string) {
	// mr-out-X-job.Jobid,   目标文件 mr-out-job.Jobid
	goal_name := "mr-out-" + strconv.Itoa(job.Jobid)
	os.Remove(goal_name)

	//需要得到文件的个数
	intermediate := make([]KeyValue, 0)
	for i := 1; i <= job.Filenum; i++ {
		filetmpname := "mr-out-" + strconv.Itoa(i) + "-" + strconv.Itoa(job.Jobid)
		filetmp, err := os.Open(filetmpname)
		if err != nil {
			log.Fatalf("cannot read %v", filetmpname)
		}
		dec := json.NewDecoder(filetmp)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		filetmp.Close()
	}
	sort.SliceStable(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	//开始写入
	goal_file, err := os.Create(goal_name)
	if err != nil {
		log.Fatalf("Reduce task create %v file failed", goal_name)
	}
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(goal_file, "%v %v\n", intermediate[i].Key, output)

		i = j
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
	// Unix 域套接字是一种在同一台计算机上的进程之间进行通信的机制。
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

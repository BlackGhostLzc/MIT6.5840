package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// import "fmt"
import "sync"
import "time"
import "strconv"

const (
	Map_phase    = iota // 0
	Reduce_phase        // 1
	Done_phase          // 2
)

const (
	Map_task     = iota // 0
	Reduce_task         // 1
	Waiting_task        // 2
)

// 任务，无论是MapJob还是ReduceJob都是这个结构体
type Job struct {
	Jobtype  int    // Waiting , Map, Reduce,
	Filename string // MapJob才会需要
	Jobid    int    // MapJob需要用到  创建nreduce 个 mr-out-Jobid-X 文件作为记录
	NReduce  int
	Filenum  int
}

type JobInfo struct {
	StartTime time.Time
	JobPtr    *Job
}

type Coordinator struct {
	// Your definitions here.
	Current_phase      int
	Reduce_num         int
	Maptask_queue      chan *Job // 虽然说chan不可传递指针，但我们只是把chan当做一个队列使用，不会有其他线程从chan中取数据
	Reducetask_queue   chan *Job
	Mapblock_record    map[int]*JobInfo // map 数据结构
	Reduceblock_record map[int]*JobInfo // map 数据结构

	Mapblock_mutex    sync.Mutex // 专门来锁 Mapblock_record这个数据结构
	Reduceblock_mutex sync.Mutex
}

func (c *Coordinator) Initializing(files []string, nReduce int) {
	c.Current_phase = Map_phase
	c.Reduce_num = nReduce
	c.Maptask_queue = make(chan *Job, 20)
	c.Reducetask_queue = make(chan *Job, 20)

	c.Mapblock_record = make(map[int]*JobInfo)
	c.Reduceblock_record = make(map[int]*JobInfo)

	for i := 0; i < len(files); i++ {
		job := Job{
			Jobtype:  Map_task,
			Filename: files[i],
			Jobid:    i + 1,
			NReduce:  nReduce,
			Filenum:  len(files),
		}
		// fmt.Printf("Add map job to maptask_queue, filename is [%s]\n", job.Filename)
		c.Maptask_queue <- &job
	}

	for i := 0; i < nReduce; i++ {
		job := Job{
			Jobtype: Reduce_task,
			Jobid:   i,
			NReduce: nReduce,
			Filenum: len(files),
		}
		// fmt.Printf("Add reduce job to reducetask_queue\n")
		c.Reducetask_queue <- &job
	}

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReceiveFinishMessage(job *Job, args *ExampleArgs) error {
	if job.Jobtype == Map_task {
		// fmt.Printf("Receive finish a Map_task message, Jobid is %v\n", job.Jobid)
		// 阻塞队列清除，还要考虑是不是超出了时间限制，上锁？
		c.Mapblock_mutex.Lock()
		defer c.Mapblock_mutex.Unlock()

		t2 := time.Now()
		t1 := c.Mapblock_record[job.Jobid].StartTime
		seconds := t2.Sub(t1).Seconds()
		// fmt.Printf("This [map] Job finish time is %v seconds\n", seconds)

		if seconds > 10 {
			// 把job再放入Maptask_queue中,不应该在这里做，而应该开一个协程来处理
			// c.Maptask_queue <- Mapblock_record[job.Jobid].JobPtr
			// fmt.Printf("Time exceeded!\n")
		} else {
			// 把任务从阻塞队列中删除
			delete(c.Mapblock_record, job.Jobid)
			// fmt.Printf("Delete a [map] task Successfully\n")
		}

	} else if job.Jobtype == Reduce_task {
		// fmt.Printf("Receive finish a Reduce_task message, Jobid is %v\n", job.Jobid)
		// 阻塞队列清除，考虑是否超出时间限制
		c.Reduceblock_mutex.Lock()
		defer c.Reduceblock_mutex.Unlock()

		t2 := time.Now()
		t1 := c.Reduceblock_record[job.Jobid].StartTime
		seconds := t2.Sub(t1).Seconds()
		// fmt.Printf("This [reduce] Job finish time is %v seconds\n", seconds)

		if seconds > 10 {
			// fmt.Printf("Time exceeded!\n")
		} else {
			// 把任务从阻塞队列中删除
			delete(c.Reduceblock_record, job.Jobid)

			// 再把中间文件删除
			// mr-out-X-job.Jobid
			for i := 1; i <= job.Filenum; i++ {
				filename := "mr-out-" + strconv.Itoa(i) + "-" + strconv.Itoa(job.Jobid)
				os.Remove(filename)
			}
			// fmt.Printf("Delete a [reduce] task Successfully\n")
		}

	}
	return nil
}

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *Job) error {
	if c.Current_phase == Map_phase {
		if len(c.Maptask_queue) == 0 {
			reply.Jobtype = Waiting_task
		} else {
			tmp := <-c.Maptask_queue
			// 加入阻塞队列
			c.Mapblock_mutex.Lock()

			jobinfo := JobInfo{
				StartTime: time.Now(),
				JobPtr:    tmp,
			}
			c.Mapblock_record[tmp.Jobid] = &jobinfo
			// fmt.Printf("Adding a Map[jobid:%d] job to Mapblock_record\n", tmp.Jobid)

			*reply = *tmp
			c.Mapblock_mutex.Unlock()
		}
	} else if c.Current_phase == Reduce_phase {
		if len(c.Reducetask_queue) == 0 {
			reply.Jobtype = Waiting_task
		} else {
			tmp := <-c.Reducetask_queue
			// 加入阻塞队列
			c.Reduceblock_mutex.Lock()

			jobinfo := JobInfo{
				StartTime: time.Now(),
				JobPtr:    tmp,
			}
			c.Reduceblock_record[tmp.Jobid] = &jobinfo
			// fmt.Printf("Adding a Reduce[jobid:%d] job to Mapblock_record\n", tmp.Jobid)

			*reply = *tmp
			c.Reduceblock_mutex.Unlock()
		}

	}
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
	ret := false

	if c.Current_phase == Done_phase {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func (c *Coordinator) CheckTimeoutTasks() {
	for {

		if c.Current_phase == Map_phase {
			// fmt.Printf("Current phase is Map_phase\n")

			c.Mapblock_mutex.Lock()

			if len(c.Maptask_queue) == 0 && len(c.Mapblock_record) == 0 {
				c.Current_phase = Reduce_phase
				continue
			}

			// 对于阻塞队列中的所有任务判断时间
			t2 := time.Now()
			for k, v := range c.Mapblock_record {
				// k->jobid        v->*JobInfo
				t1 := v.StartTime
				seconds := t2.Sub(t1).Seconds()
				if seconds > 10 {
					// fmt.Printf("CheckTimeoutTasks find a Map task time exceeded!\n")
					c.Maptask_queue <- v.JobPtr
					delete(c.Mapblock_record, k)
				}
			}

			c.Mapblock_mutex.Unlock()
		} else if c.Current_phase == Reduce_phase {
			// fmt.Printf("Current phase is Reduce_phase \n")

			c.Reduceblock_mutex.Lock()

			if len(c.Reducetask_queue) == 0 && len(c.Reduceblock_record) == 0 {
				c.Current_phase = Done_phase
				// Exit(0)
			}

			t2 := time.Now()
			for k, v := range c.Reduceblock_record {
				// k->jobid        v->*JobInfo
				t1 := v.StartTime
				seconds := t2.Sub(t1).Seconds()
				if seconds > 10 {
					// fmt.Printf("CheckTimeoutTasks find a Map task time exceeded!\n")
					c.Reducetask_queue <- v.JobPtr
					delete(c.Reduceblock_record, k)
				}
			}

			c.Reduceblock_mutex.Unlock()
		} else {
			// fmt.Printf("I am CheckTimeoutTasks, my task is finished\n")
			time.Sleep(2 * time.Second)
		}

		time.Sleep(2 * time.Second)
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 接着要初始化 coordinator
	// fmt.Printf("Initializing Coordinator...\n")
	c.Initializing(files, nReduce)

	go c.CheckTimeoutTasks()

	c.server()
	return &c
}

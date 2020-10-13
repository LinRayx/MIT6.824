package mr

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

const (
	IDLE = 0
	PROCESS = 1
	FINISHED = 2
	ALIVE = 3
	DEAD = 4
)

type WorkState struct {
	WorkerId int
	WorkerState int
}

type Master struct {
	// Your definitions here.
	nTask int
	mapFinsh int
	reduceFinish int
	worker []WorkState
	workers int
	STATE int
	mapPtr int
	NReduces int
	reducePtr int
	intermediate [][]string
	workDeadNum int
	reduceTask []ReduceTask
	idleTask chan int
	Datas []MapTask
	mutex sync.Mutex
}

type MapTask struct {
	Filename string
	flag int
	worker int
}

type ReduceTask struct {
	reduceTask []string
	flag int
	worker int
}




// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	// Remove removes the named file or directory.
	// If there is an error, it will be of type *PathError.
	os.Remove(sockname)
	// Listen announces on the local network address laddr.
	// The network net must be a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
	// 本地通讯
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// Serve accepts incoming HTTP connections on the listener l,
	// creating a new service goroutine for each.
	// The service goroutines read requests and then call handler to reply to them.
	// Handler is typically nil, in which case the DefaultServeMux is used.
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	ret := false
	if m.STATE == ALLTASKDONE  {
		ret = true
		//err := os.Remove(masterSock())
		//if err != nil {
		//	log.Fatalf("Remove masterSock error ,%v", err.Error())
		//}
	} else {
		ret = false
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Println("MakeMaster")
	m := Master{}

	// Your code here.
	m.idleTask = make(chan int, len(files))
	m.nTask = 0
	for _, filename := range files {
		m.Datas = append(m.Datas, MapTask{Filename: filename, flag: IDLE })
		m.idleTask <- m.nTask
		m.nTask++
	}
	m.NReduces = nReduce
	m.mapPtr = 0
	m.reducePtr = 0
	m.reduceTask = make([]ReduceTask, nReduce)

	m.server()

	return &m
}

func (m *Master) GetTask(args *WorkArgs, reply *WorkReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	log.Printf("GetTask# workerId : %d mapFinsh: %d reduceFinish: %d m.nTask: %d m.STATE: %d WorkerState: %d mapPtr:%d reducePtr: %d\n",
		args.WorkerId, m.mapFinsh, m.reduceFinish, m.nTask, m.STATE, m.worker[args.WorkerId].WorkerState, m.mapPtr, m.reducePtr)
	log.Println(len(m.idleTask))
	if m.worker[args.WorkerId].WorkerState == DEAD {
		reply.Status = DUMP
		return nil
	}
	if m.STATE == ALLTASKDONE {
		reply.Status = ALLTASKDONE
		return nil
	}
	if m.mapPtr < m.nTask {
		_ = m.GetMapTask(args, reply)
		m.mapPtr++
		m.STATE = MAPING

	} else {
		// map 还未全部结束
		if m.STATE == MAPING  {
			reply.Status = m.STATE
		} else if m.reducePtr < m.NReduces {
			_ = m.GetReduceTask(args, reply)
			m.reducePtr++
			m.STATE = REDUCING
		} else {
			if m.STATE == REDUCEFINISHED {
				reply.Status = ALLTASKDONE
				m.STATE = ALLTASKDONE
			} else {
				reply.Status = REDUCING
			}
		}
	}
	return nil
}

func (m *Master) GetMapTask(args *WorkArgs, reply *WorkReply) error {

	task := <-m.idleTask
	log.Printf("GetMapTask %d\n", task)
	reply.Filename = m.Datas[task].Filename
	m.Datas[task].flag = PROCESS
	m.Datas[task].worker = args.WorkerId
	reply.NReduce = m.NReduces
	reply.Status = MAPTASK
	reply.MapNumber = task
	go m.testMapTaskAlive(task)
	return nil
}

func (m *Master) GetReduceTask(args *WorkArgs, reply *WorkReply) error {
	log.Printf("GetReduceTask reducePtr: %d\n", m.reducePtr)
	taskNumber := <-m.idleTask
	reply.NReduce = m.NReduces
	reply.Status = REDUCETASK
	reply.ReduceNumber = taskNumber
	reply.Inv = m.nTask
	reply.ReduceFiles = m.reduceTask[taskNumber].reduceTask
	m.reduceTask[taskNumber].worker = args.WorkerId
	m.reduceTask[taskNumber].flag = PROCESS
	go m.testReduceTaskAlive(taskNumber)
	return nil
}

func (m *Master) MapFinish(args *WorkArgs, reply *WorkReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.worker[args.WorkerId].WorkerState == DEAD {
		reply.Status = DUMP
		return nil
	}
	log.Printf("MapFinish: %d workerId: %d\n", args.MapNumber, args.WorkerId)
	if m.Datas[args.MapNumber].flag == PROCESS {
		m.mapFinsh++
		m.Datas[args.MapNumber].flag = FINISHED
		m.intermediate[args.WorkerId] = append(m.intermediate[args.WorkerId], args.Intermediata...)
	}
	//log.Println(args.Intermediata)
	if m.mapFinsh == m.nTask {
		m.STATE = MAPFINISHED
		for _, inter := range m.intermediate {
			//log.Printf("number: %d inter: %v", len(inter), inter)
			for _, filename := range inter {
				// mr-MapNumber-ReduceNumber-Worker
				a := 0
				b := 0
				cnt := 0
				for i, c := range filename {
					if c == '-' {
						cnt++
						if cnt == 2 {
							a = i+1
						}
						if cnt == 3 {
							b = i
							break
						}

					}
				}
				reduceNum, _ := strconv.Atoi(filename[a:b])
				m.reduceTask[reduceNum].reduceTask =
					append(m.reduceTask[reduceNum].reduceTask, filename)
				m.reduceTask[reduceNum].flag = IDLE
			}
		}
		log.Println(m.reduceTask)
		m.idleTask = make(chan int, m.NReduces)
		for i := 0; i < m.NReduces; i++ {
			m.idleTask <- i
		}
	}
	return nil
}

func (m *Master) ReduceFinish(args *WorkArgs, reply *WorkReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.worker[args.WorkerId].WorkerState == DEAD {
		reply.Status = DUMP
		return nil
	}
	log.Printf("ReduceFinish %d workerId: %d\n", args.ReduceNumber, args.WorkerId)

	m.reduceFinish++
	m.reduceTask[args.ReduceNumber].flag = FINISHED
	if m.reduceFinish == m.NReduces {
		m.STATE = REDUCEFINISHED
	}
	return nil
}

func (m *Master) RegisterWorker(args *WorkArgs, reply *WorkReply) error {
	log.Println("RegisterWorker")
	m.mutex.Lock()
	defer m.mutex.Unlock()
	reply.Status = REGISTERED
	m.intermediate = append(m.intermediate, []string{})
	m.worker = append(m.worker, WorkState{
		WorkerId:    m.workers,
		WorkerState: ALIVE,
	})
	reply.WorkerId = m.workers
	m.workers++
	return nil
}

func (m *Master) QuitWorker(args *WorkArgs, reply *WorkReply) error {
	log.Println("QuitWorker")
	m.mutex.Lock()
	m.workers--
	m.mutex.Unlock()
	return nil
}

func (m *Master) testMapTaskAlive(taskNumber int) {
	time.Sleep(10 * time.Second)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.STATE != MAPING {
		return
	}
	if m.Datas[taskNumber].flag == PROCESS {
		workId := m.Datas[taskNumber].worker
		log.Printf("testMapTaskAlive taskNumber: %d workerId: %d---------\n", taskNumber, workId)
		m.worker[workId].WorkerState = DEAD
		m.workDeadNum++
		m.intermediate[workId] = nil
		for i, v := range m.Datas {
			if v.worker == workId {
				log.Printf("tasknumber: %d flag %d\n", i, v.flag)
				if v.flag == FINISHED {
					m.mapFinsh--
				}
				if v.flag == FINISHED || v.flag == PROCESS {
					m.mapPtr--
					m.idleTask <- i
				}
				m.Datas[i].flag = IDLE // v只是值传递

			}
		}
		log.Printf("----------------------------------------- chan len: %d\n", len(m.idleTask))
	}

}

func (m *Master) testReduceTaskAlive(taskNumber int) {
	time.Sleep(10 * time.Second)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.STATE != REDUCING {
		return
	}
	if m.reduceTask[taskNumber].flag == PROCESS {
		m.reduceTask[taskNumber].flag = IDLE
		workId := m.reduceTask[taskNumber].worker
		log.Printf("testReduceTaskAlive taskNumber: %d", taskNumber)
		m.worker[workId].WorkerState = DEAD
		m.workDeadNum++
		m.reducePtr--
		m.idleTask <- taskNumber
	}
}

func (m *Master) checkWorkerState(args *WorkArgs, reply *WorkReply) bool {
	if m.worker[args.WorkerId].WorkerState == DEAD {
		reply.Status = DUMP
		return false
	}
	return true
}
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// asking master for a task
	args := WorkArgs{}
	reply := WorkReply{}
	//log.Println("RegisterWorker begin")
	for call("Master.RegisterWorker", &args, &reply) == false {
		time.Sleep(time.Second)
	}

	args.WorkerId = reply.WorkerId
	log.Printf("RegisterWorker successful %d\n", args.WorkerId)
	runing := true
	for runing {
		switch reply.Status {
		case REGISTERED:
			call("Master.GetTask", &args, &reply)
			break
		case MAPTASK:
			//log.Printf("reply: %v\n", reply)
			args.MapNumber = reply.MapNumber
			args.Intermediata = args.Intermediata[:0]
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)

			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			ofiles := make(map[string]*os.File)
			var i int
			for i = 0; i < reply.NReduce; i++ {
				oname := "mr-"+strconv.Itoa(reply.MapNumber)+"-"+strconv.Itoa(i)+"-"+strconv.Itoa(args.WorkerId)
				ofile, err := os.Create(oname)
				ofiles[oname] = ofile
				if err != nil {
					log.Fatalf("create %s file error %v", oname, err.Error())
				}
			}

			kva := mapf(reply.Filename, string(content))

			for _, kv := range kva {
				oname := "mr-"+strconv.Itoa(reply.MapNumber)+"-"+strconv.Itoa(ihash(kv.Key)%reply.NReduce)+"-"+strconv.Itoa(args.WorkerId)
				//log.Printf("%v %v\n",oname,kv)
				ofile := ofiles[oname]
				enc := json.NewEncoder(ofile)
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("encode err: %v %v", err.Error(), kv)
				}

			}
			for _, file := range ofiles {
				//log.Printf("MAPTASK filename: %s\n", file.Name())
				args.Intermediata = append(args.Intermediata, file.Name())
				file.Close()
			}
			call("Master.MapFinish", &args, &reply)
			call("Master.GetTask", &args, &reply)
			break
		case REDUCETASK:
			//log.Printf("reply: %v\n", reply)
			i := 0
			//inv := reply.Inv
			args.ReduceNumber = reply.ReduceNumber
			intermedia := []KeyValue{}

			log.Printf("REDUCETASK reduceNumber: %d files: : %v\n", reply.ReduceNumber, reply.ReduceFiles)
			for _, oname := range reply.ReduceFiles {

				//oname := "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(reply.ReduceNumber)
				ofile, err := os.Open(oname)
				if err != nil {
					log.Fatal("open %s error %v", oname, err)
				}
				dec := json.NewDecoder(ofile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermedia = append(intermedia, kv)
				}
				ofile.Close()
			}

			sort.Sort(ByKey(intermedia))
			oname := "mr-out-"+strconv.Itoa(reply.ReduceNumber)

			//ofile, _ := os.Create(oname)
			ofile, _ := ioutil.TempFile("", oname)
			i = 0
			for i < len(intermedia) {
				j := i + 1
				for j < len(intermedia) && intermedia[i].Key == intermedia[j].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermedia[k].Value)
				}
				output := reducef(intermedia[i].Key, values)
				_, _ = fmt.Fprintf(ofile, "%v %v\n", intermedia[i].Key, output)
				i = j
			}

			ofile.Close()
			os.Rename(ofile.Name(), oname)
			log.Println(ofile.Name())
			call("Master.ReduceFinish", &args, &reply)
			call("Master.GetTask", &args, &reply)
			break
		case MAPING:
			time.Sleep(time.Second)
			call("Master.GetTask", &args, &reply)
			break
		case REDUCING:
			time.Sleep(time.Second)
			call("Master.GetTask", &args, &reply)
			break
		case ALLTASKDONE:
			log.Printf("workerId: %d ALLTASKDONE\n", args.WorkerId)
			call("Master.QuitWorker", &args, &reply)
			runing = false
			break
		case DUMP:
			log.Printf("workerId: %d DUMP\n", args.WorkerId)
			runing = false
		default:
			runing = false
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	log.Fatal(err.Error())
	return false
}

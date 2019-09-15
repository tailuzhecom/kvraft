package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	reduce_map := make(map[string][]string)
	// sort，每个key，对应一个[]string，然后将每个key和对应的value传递给reduce处理
	for i := 0; i < nMap; i++ {
		tmp_file_name := reduceName(jobName, i, reduceTask)	// 收集每一个maptask产生的对应reduceTask的结果
		tmp_file, e := os.Open(tmp_file_name)
		if e != nil {
			fmt.Println(e)
		}

		decoder := json.NewDecoder(tmp_file)
		var kvs []KeyValue
		decode := decoder.Decode(&kvs)
		if decode != nil {
			fmt.Println("get json error")
		}
		tmp_file.Close()
		for _, kv := range kvs {
			reduce_map[kv.Key] = append(reduce_map[kv.Key], kv.Value)
		}
	}

	// 一个reduceTask可能要处理多个key,对reduce task中的每一个key进行处理
	reduce_file, e := os.Create(mergeName(jobName, reduceTask))  // mrtmp.test-res-n
	defer reduce_file.Close()
	for k, v := range reduce_map {
		reduce_res := reduceF(k, v)

		if e != nil {
			fmt.Println(e)
		}
		encoder := json.NewEncoder(reduce_file)
		kv := KeyValue{k, reduce_res}

		encode := encoder.Encode(kv)
		if encode != nil {
			fmt.Println(encode)
		}
		log.Println("do_reduce()", reduce_res)
	}



}
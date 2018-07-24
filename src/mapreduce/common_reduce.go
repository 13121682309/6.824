package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

type ByKey []KeyValue

func (kv ByKey) Len() int           { return len(kv) }
func (kv ByKey) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv ByKey) Less(i, j int) bool { return kv[i].Key < kv[j].Key }

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

	var items []KeyValue
	for mapTask := 0; mapTask < nMap; mapTask++ {
		inFile := reduceName(jobName, mapTask, reduceTask)
		in, err := os.Open(inFile)
		if err != nil {
			fmt.Printf("ReduceTask %d fail, err info: %s\n", reduceTask, err)
			return
		}
		fmt.Println(inFile)
		defer in.Close()
		dec := json.NewDecoder(in)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Printf("ReduceTask %d fail, err info: %s\n", reduceTask, err)
				return
			}
			items = append(items, kv)
		}
	}

	sort.Sort(ByKey(items))

	out, err := os.Create(outFile)
	if err != nil {
		fmt.Printf("ReduceTask %d fail, err info: %s\n", reduceTask, err)
		return
	}
	defer out.Close()
	enc := json.NewEncoder(out)

	for i := 0; i < len(items); {
		key := items[i].Key
		var values []string
		var j int
		for j = i; j < len(items) && items[j].Key == key; j++ {
			values = append(values, items[j].Value)
		}
		enc.Encode(&KeyValue{key, reduceF(key, values)})
		i = j
	}
}

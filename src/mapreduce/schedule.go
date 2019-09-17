package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce

	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	//wg.Add(ntasks)
	//for i := 0; i < ntasks; i++ {
	//	worker_addr := <- registerChan
	//	func_arg := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
	//
	//	go func(worker_addr string, func_arg DoTaskArgs) {
	//		defer wg.Done()
	//		for {
	//			ret := call(worker_addr, "Worker.DoTask", func_arg, nil)
	//			if ret {  // 成功返回，否则一直运行到成功为止
	//				go func(registerChan chan string, worker_addr string) {  // 这里要使用goroutine
	//					registerChan <- worker_addr // worker变为可用状态
	//				}(registerChan, worker_addr)
	//				break
	//			}
	//		}
	//
	//	}(worker_addr, func_arg)
	//}

	wg.Add(ntasks)
	taskChan := make(chan int, ntasks)
	// 把任务放入任务队列
	for i:=0;i<ntasks;i++  {
		taskChan <- i
	}

	// 从worker队列中获取可用的worker，并在一个gorountine中一直占用这个worker
	// worker会监测是否有任务需要完成，每取到一个task，就会去执行一次，成功告知WaitGroup，
	// 失败将任务返回任务队列
	go func() {
		for {
			worker_addr := <- registerChan
			go func(worker_addr string) {
				for {
					i := <- taskChan
					if call(worker_addr,"Worker.DoTask", &DoTaskArgs{jobName,
						mapFiles[i],phase,i,n_other},nil){
						wg.Done()
					} else{
						taskChan <- i
					}
				}
			}(worker_addr)
		}
	}()

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

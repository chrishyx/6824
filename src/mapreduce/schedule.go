package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	taskQueue := make(chan int, ntasks)
	success := make(chan int, ntasks)
	go func() {
		for i := 0; i < ntasks; i++ {
			<-success
		}
		taskQueue <- -1
	}()

	for i := 0; i < ntasks; i++ {
		taskQueue <- i
	}

	for {
		index := <-taskQueue
		if index == -1 {
			break
		}
		work := <-mr.registerChannel
		args := DoTaskArgs{mr.jobName, mr.files[index], phase, index, nios}
		go func(work string, args DoTaskArgs, registerChannel chan string) {
			ok := call(work, "Worker.DoTask", args, new(struct{}))
			if ok == false {
				taskQueue <- args.TaskNumber
				fmt.Printf("Task Failed: %v\n", args)
			} else {
				success <- 1
				registerChannel <- work
			}
		}(work, args, mr.registerChannel)
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

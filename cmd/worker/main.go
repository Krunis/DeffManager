package main

import (
	"flag"

	"github.com/Krunis/DeffManager/packages/worker"
)
var (
	coordinatorPort = flag.String("coordinator", ":8080", "Network address of the Coordinator.")
	workerPort = flag.String("worker_port", "", "Port on which the Worker server serves requests.")
)
func main() {
	flag.Parse()
	worker := worker.NewWorkerServer(*coordinatorPort, *workerPort)

	worker.Start()

	worker.Stop()
}
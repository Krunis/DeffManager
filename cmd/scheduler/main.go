package main

import (
	"log"

	"github.com/Krunis/DeffManager/packages/common"
	"github.com/Krunis/DeffManager/packages/scheduler"
)

func main() {
	dbConnectionString := common.GetDBConnectionString()
	
	schedulerServer := scheduler.NewServer(":8081", dbConnectionString)

	err := schedulerServer.Start()

	if err != nil{
		log.Fatalf("Error scheduler server: %s", err.Error())
	}

	log.Println("Scheduler server stopped")
}
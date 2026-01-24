package main

import (
	"github.com/Krunis/DeffManager/packages/common"
	"github.com/Krunis/DeffManager/packages/coordinator"
)

func main() {
	coordinator := coordinator.NewCoordinatorServer(":8080", common.GetDBConnectionString())
	
	coordinator.Start()

	coordinator.Stop()
}
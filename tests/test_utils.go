package tests

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Krunis/DeffManager/packages/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Krunis/DeffManager/packages/grpcapi"
	"github.com/Krunis/DeffManager/packages/scheduler"
	"github.com/Krunis/DeffManager/packages/worker"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Cluster struct {
	coordinator        *coordinator.CoordinatorServer
	coordinatorAddress string
	scheduler          *scheduler.SchedulerServer
	workers            []*worker.WorkerServer
	database           testcontainers.Container
}

const (
	postgresUser     = "test_user"
	postgresDB       = "test_scheduler"
	postgresHost     = "localhost"
	postgresPassword = "chlen"
)

var postgresPort string

func (c *Cluster) LaunchCluster(schedulerPort string, coordinatorPort string, countWorkers uint8) {
	var err error

	if err = c.createDatabase(); err != nil {
		log.Fatalf("Could not launch DB container: %+v", err)
	}

	c.scheduler = scheduler.NewServer(schedulerPort, getDbConnectionString())
	startServer(c.scheduler)

	c.coordinatorAddress = coordinatorPort
	c.coordinator = coordinator.NewCoordinatorServer(coordinatorPort, getDbConnectionString())
	startServer(c.coordinator)

	c.workers = make([]*worker.WorkerServer, countWorkers)
	for i := 0; i < int(countWorkers); i++ {
		log.Println(c.coordinatorAddress)
		c.workers[i] = worker.NewWorkerServer("", coordinatorPort)
		startServer(c.workers[i])
	}

	c.waitForWorkers()
}

func (c *Cluster) StopCluster(){
	for i := 0; i < len(c.workers); i++{
		stopServer(c.workers[i])
	}

	stopServer(c.coordinator)

	stopServer(c.scheduler)

	if err := c.database.Terminate(context.Background()); err != nil{
		log.Printf("Failed to terminate DB container: %v\n", err)
	}
}


func (c *Cluster) createDatabase() error {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "scheduler-postgres",
		Name:         "DeffManager-test",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_DB":       postgresDB,
			"POSTGRES_PASSWORD": postgresPassword,
		},
		WaitingFor: wait.ForListeningPort("5432/tcp"),
	}

	var err error
	c.database, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}

	logs, _ := c.database.Logs(ctx)
	io.Copy(os.Stdout, logs)

	port, err := c.database.MappedPort(context.Background(), "5432/tcp")
	if err != nil {
		log.Println(err.Error())
	}

	postgresPort = port.Port()
	return err
}

func getDbConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		postgresUser, postgresPassword, postgresHost, postgresPort, postgresDB)
}

func (c *Cluster) waitForWorkers() {
	for {
		c.coordinator.WorkerPoolMutex.Lock()
		c.coordinator.WorkerPoolKeysMutex.RLock()

		if len(c.workers) == len(c.coordinator.WorkerPool) {
			c.coordinator.WorkerPoolKeysMutex.RUnlock()
			c.coordinator.WorkerPoolMutex.Unlock()
			break
		}

		c.coordinator.WorkerPoolKeysMutex.RUnlock()
		c.coordinator.WorkerPoolMutex.Unlock()
		time.Sleep(time.Second)
	}
}

func startServer(srv interface {
	Start() error
}) {
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Failed to start server s: %v", err)
		}
	}()
}

func stopServer(srv interface {
	Stop()
}) {
	srv.Stop()
}

func createTestClient(coordinatorAddress string) (*grpc.ClientConn, pb.CoordinatorServiceClient){
	conn, err := grpc.NewClient(coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil{
		log.Fatal("Could not create test connection to coordinator")
	}

	return conn, pb.NewCoordinatorServiceClient(conn)
}
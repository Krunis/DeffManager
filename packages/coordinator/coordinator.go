package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Krunis/DeffManager/packages/common"
	pb "github.com/Krunis/DeffManager/packages/grpcapi"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const(
	scanInterval = time.Second * 5
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer

	port       string
	grpcServer *grpc.Server
	lis        net.Listener

	dbpool             *pgxpool.Pool
	dbConnectionString string

	workerPool          map[uint32]*workerInfo
	workerPoolMutex     sync.Mutex
	workerPoolKeys      []uint32
	workerPoolKeysMutex sync.RWMutex

	roundRobinIndex uint32

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	stopOnce sync.Once

}

type workerInfo struct {
	heartbeatMisses uint8
	address         string
	grpcConn        *grpc.ClientConn
	client          pb.WorkerServiceClient
}

func NewCoordinatorServer(port string, dbConnectionString string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &CoordinatorServer{
		port:               port,
		dbConnectionString: dbConnectionString,
		workerPool: make(map[uint32]*workerInfo),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (c *CoordinatorServer) Start() {
	var err error

	c.dbpool, err = common.ConnectToDB(c.ctx, c.dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s", err)
	}

	if err = c.startGRPCServer(); err != nil {
		log.Fatalf("Failed to start gRPC: %s", err)
	}

	go c.pollingDB()

	go c.manageWorkerPool()

	c.awaitShutdown()
}

func (c *CoordinatorServer) startGRPCServer() error {
	var err error

	c.lis, err = net.Listen("tcp", c.port)
	if err != nil {
		return err
	}

	log.Printf("Starting GRPC server on port %s...\n", c.port)

	c.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(c.grpcServer, c)

	go func() {
		if err = c.grpcServer.Serve(c.lis); err != nil {
			log.Fatalf("GRPC server coordinator failed: %s", err)
		}
	}()

	return err
}

func (c *CoordinatorServer) pollingDB() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.wg.Go(func() {
				c.executeDefferedTasks()
			})
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *CoordinatorServer) executeDefferedTasks() {
	tx, err := c.dbpool.Begin(context.Background())
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	defer func() {
		if err := tx.Rollback(ctx); err != nil && err.Error() != pgx.ErrTxClosed.Error() {
			log.Printf("Failed to rollback transaction: %s\n", err)
		}
	}()

	rows, err := tx.Query(ctx,
		`SELECT id, command
		FROM tasks
		WHERE scheduled_at < NOW() + INTERVAL '1 second'
		AND picked_at IS NULL
		AND (must_be_execute_at IS NULL OR must_be_execute_at <= NOW())
		ORDER BY (must_be_execute_at IS NULL),
		must_be_execute_at,
		scheduled_at
		FOR UPDATE SKIP LOCKED`,
	)
	if err != nil {
		log.Printf("Failed to query: %s", err)
	}
	
	var id, command string
	var tasks = []*pb.TaskRequest{}

	for rows.Next() {
		if err := rows.Scan(&id, &command); err != nil {
			log.Printf("Failed to scan row: %s", err)
		}
		tasks = append(tasks, &pb.TaskRequest{TaskId: id, Command: command})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v\n", err)
	}

	rows.Close()

	for _, task := range tasks {

		_, err := tx.Exec(ctx, `UPDATE tasks SET picked_at = NOW() WHERE id = $1`, task.GetTaskId())

		if err != nil {
			log.Printf("Failed to update picked_at for %s: %s", task.GetTaskId(), err)
		}

		if err := c.submitTaskToWorker(task); err != nil {
			log.Printf("Failed to submit task %s: %s", task.GetTaskId(), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v\n", err)
	}

}

func (c *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := c.getNextWorker()
	if worker == nil {
		return errors.New("no workers available")
	}

	_, err := worker.client.SubmitTask(context.Background(), task)
	return err
}

func (c *CoordinatorServer) getNextWorker() *workerInfo {
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()

	if len(c.workerPool) == 0 {
		return nil
	}

	worker := c.workerPool[c.workerPoolKeys[c.roundRobinIndex%uint32(len(c.workerPool))]]

	c.roundRobinIndex++
	return worker
}

func (c *CoordinatorServer) awaitShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(stop)

	<-stop

	c.Stop()
}

func (c *CoordinatorServer) Stop() {
	c.stopOnce.Do(func() {
		if c.grpcServer != nil {
			c.grpcServer.GracefulStop()
		}

		c.cancel()

		c.wg.Wait()

		if c.lis != nil {
			c.lis.Close()
		}

		c.workerPoolMutex.Lock()

		for _, worker := range c.workerPool {
			if worker.grpcConn != nil {
				if err := worker.grpcConn.Close(); err != nil {
					log.Printf("Failed to close client conn with %s: %s\n", worker.address, err)
				}
			}
		}

		c.workerPoolMutex.Unlock()

		c.dbpool.Close()
		log.Println("Database pool stopped")
	})

}

func (c *CoordinatorServer) manageWorkerPool() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.wg.Go(func() {
				c.removeInactiveWorkers()
			})
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *CoordinatorServer) removeInactiveWorkers() {
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()

	for workerID, worker := range c.workerPool {
		if worker.heartbeatMisses > 1 {
			log.Printf("Removing inactive worker: %d\n", workerID)

			worker.grpcConn.Close()
			delete(c.workerPool, workerID)

			c.workerPoolKeysMutex.Lock()

			c.workerPoolKeys = make([]uint32, 0, len(c.workerPool))

			for key := range c.workerPool {
				c.workerPoolKeys = append(c.workerPoolKeys, key)
			}

			c.workerPoolKeysMutex.Unlock()
		} else {
			worker.heartbeatMisses++
		}
	}
}

func (c *CoordinatorServer) SendHeartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	c.workerPoolMutex.Lock()
	defer c.workerPoolMutex.Unlock()

	workerID := r.GetWorkerId()

	if worker, ok := c.workerPool[workerID]; ok {
		worker.heartbeatMisses = 0

		log.Printf("Reset heartbeat miss for worker: %d\n", workerID)
	} else {
		log.Printf("Registering worker: %d\n", workerID)
		conn, err := grpc.NewClient(r.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		c.workerPool[workerID] = &workerInfo{
			address:  r.GetAddress(),
			grpcConn: conn,
			client:   pb.NewWorkerServiceClient(conn),
		}

		c.workerPoolKeysMutex.Lock()
		defer c.workerPoolKeysMutex.Unlock()

		c.workerPoolKeys = make([]uint32, 0, len(c.workerPool))
		for key := range c.workerPool {
			c.workerPoolKeys = append(c.workerPoolKeys, key)
		}

		log.Printf("Registered worker: %d\n", workerID)
	}
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (c *CoordinatorServer) UpdateTaskStatus(ctx context.Context, r *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	var timestamp time.Time
	var column string

	status := r.GetStatus()
	taskID := r.GetTaskId()

	switch status {
	case pb.TaskStatus_STARTED:
		timestamp = time.Unix(r.GetStartedAt(), 0)
		column = "started_at"
	case pb.TaskStatus_COMPLETED:
		timestamp = time.Unix(r.GetCompletedAt(), 0)
		column = "completed_at"
	case pb.TaskStatus_FAILED:
		timestamp = time.Unix(r.GetFailedAt(), 0)
		column = "failed_at"
	default:
		log.Println("Invalid status in UpdateTaskRequest")
		return nil, errors.ErrUnsupported
	}

	sqlStatement := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", column)

	_, err := c.dbpool.Exec(ctx, sqlStatement, timestamp, taskID)
	if err != nil {
		fmt.Printf("Failed to update task status for task %s: %s", taskID, err)
		return nil, err
	}

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

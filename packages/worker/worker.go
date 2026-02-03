package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/Krunis/DeffManager/packages/grpcapi"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const(
	heartbeatInterval = time.Second * 5
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer

	id uint32

	port       string
	lis        net.Listener
	grpcServer *grpc.Server

	coordinatorAddress string
	coordinatorClient  pb.CoordinatorServiceClient
	coordinatorConn    *grpc.ClientConn

	taskQueue     chan *pb.TaskRequest
	ReceivedTasks []*pb.TaskRequest
	ReceivedTasksMutex sync.Mutex

	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	stopOnce sync.Once
}

func NewWorkerServer(coordinatorAddress string, workerPort string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerServer{
		id:                 uuid.New().ID(),
		port:               workerPort,
		coordinatorAddress: coordinatorAddress,
		taskQueue:          make(chan *pb.TaskRequest),
		ReceivedTasks:      make([]*pb.TaskRequest, 0),
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (w *WorkerServer) Start() error{
	go w.createWorker()

	if err := w.connectToCoordinator(); err != nil {
		log.Fatalf("failed to connect to coordinator: %s", err)
	}

	if err := w.startGRPCServer(); err != nil {
		log.Fatalf("gRPC server start failed: %s", err)
	}

	go w.sendHeartbeat()

	w.awaitShutdown()
	
	return nil
}

func (w *WorkerServer) startGRPCServer() error {
	var err error

	if w.port == "" {
		w.lis, err = net.Listen("tcp", ":0")
		if err != nil {
			return err
		}
	}else{
		w.lis, err = net.Listen("tcp", w.port)
		if err != nil {
			return err
		}
	}

	w.port = fmt.Sprintf(":%d", w.lis.Addr().(*net.TCPAddr).Port)

	log.Printf("Starting worker server on %s...\n", w.port)

	w.grpcServer = grpc.NewServer()

	pb.RegisterWorkerServiceServer(w.grpcServer, w)

	go func() {
		if err := w.grpcServer.Serve(w.lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("GRPC worker server failed: %v", err)
		}
	}()

	return nil
}

//Connecting to coordinator 
func (w *WorkerServer) connectToCoordinator() error {
	conn, err := grpc.NewClient(w.coordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	w.coordinatorConn = conn

	w.coordinatorClient = pb.NewCoordinatorServiceClient(conn)
	log.Printf("Coordinator GRPC client initialized")

	return nil
}

func (w *WorkerServer) createWorker() {
	w.wg.Add(5)

	for i := 0; i < 5; i++ {
		go w.workerCell()
	}
}

func (w *WorkerServer) workerCell() {
	defer w.wg.Done()
	for {
		select {
		case task := <-w.taskQueue:
			go w.updateTaskStatus(task, pb.TaskStatus_STARTED)
			if err := w.processTask(task); err != nil {
				log.Printf("Process task break: %s", err)
				go w.updateTaskStatus(task, pb.TaskStatus_FAILED)
				continue
			}
			go w.updateTaskStatus(task, pb.TaskStatus_COMPLETED)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) processTask(r *pb.TaskRequest) error {
	log.Printf("Processing task %s...\n", r.GetTaskId())

	time.Sleep(time.Second * 5)
	if r.Command == "examplefail" {
		return fmt.Errorf("failed to process task: %s", r.TaskId)
	}

	log.Printf("Completed task %s\n", r.GetTaskId())

	return nil
}

func (w *WorkerServer) sendHeartbeat() {
	workerAddress := os.Getenv("WORKER_ADDRESS")

	if workerAddress == ""{
		workerAddress = w.lis.Addr().String()
	}else{
		workerAddress += w.port
	}
	
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := w.coordinatorClient.SendHeartbeat(w.ctx, &pb.HeartbeatRequest{
				WorkerId: w.id,
				Address: workerAddress,
			})
			if err != nil {
				log.Printf("Failed to send heartbeat for worker %d: %s\n", w.id, err)
				continue
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WorkerServer) updateTaskStatus(r *pb.TaskRequest, status pb.TaskStatus) {
	updateRequest := pb.UpdateTaskStatusRequest{}
	updateRequest.TaskId = r.TaskId

	switch status {
	case pb.TaskStatus_STARTED:
		updateRequest.StartedAt = time.Now().Unix()
	case pb.TaskStatus_COMPLETED:
		updateRequest.CompletedAt = time.Now().Unix()
	case pb.TaskStatus_FAILED:
		updateRequest.FailedAt = time.Now().Unix()
	}
	updateRequest.Status = status

	_, err := w.coordinatorClient.UpdateTaskStatus(context.Background(), &updateRequest)
	if err != nil {
		log.Fatalf("Error while updating task status for task %s: %s", updateRequest.TaskId, err)
	}
}

func (w *WorkerServer) SubmitTask(ctx context.Context, r *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("Received task: %s", r.GetTaskId())

	w.ReceivedTasks = append(w.ReceivedTasks, r)
	w.taskQueue <- r

	return &pb.TaskResponse{TaskId: r.GetTaskId(), Success: true}, nil
}

func (w *WorkerServer) awaitShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	<-stop

	w.Stop()
}

func (w *WorkerServer) Stop() {
	w.stopOnce.Do(func() {
		if w.grpcServer != nil {
			w.grpcServer.GracefulStop()
		}

		w.cancel()

		w.wg.Wait()

		if w.lis != nil{
			w.lis.Close()
		}

		if w.coordinatorConn != nil {
			if err := w.coordinatorConn.Close(); err != nil {
				log.Printf("Failed to close connection to coordinator: %s", err)
			}
		}
	})
}

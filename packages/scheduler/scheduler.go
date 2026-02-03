package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Krunis/DeffManager/packages/common"
	"github.com/jackc/pgx/v4/pgxpool"
)

var stopOnce sync.Once

type SchedulerServer struct {
	port       string
	httpServer *http.Server

	mux *http.ServeMux

	dbConnectionString string
	dbPool             *pgxpool.Pool

	ctx    context.Context
	cancel context.CancelFunc
}

type CommandRequest struct {
	Command          string  `json:"command"`
	MustBeExecute_At *string `json:"must_be_execute_at"`
	Scheduled_At     string  `json:"scheduled_at"`
}

type Task struct {
	ID              string
	Command         string
	MustBeExecuteAt *time.Time
	ScheduledAt     *time.Time
	PickedAt        *time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	FailedAt        *time.Time
}

func NewServer(port string, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())

	mux := http.NewServeMux()

	return &SchedulerServer{
		port:               port,
		mux:                mux,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = common.ConnectToDB(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	s.mux.HandleFunc("/schedule", s.handlePostTask)
	s.mux.HandleFunc("/status/", s.handleGetTask)

	s.httpServer = &http.Server{Addr: s.port, Handler: s.mux}

	
	errCh := make(chan error, 1)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	go s.awaitShutdown()

	select {
	case err = <-errCh:
		return err
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *SchedulerServer) handlePostTask(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
		log.Println("Request cancelled (shutdown or client disconnected)")
		return

	default:
		if r.Method != "POST" {
			http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		var commandReq CommandRequest

		if err := json.NewDecoder(r.Body).Decode(&commandReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("Received schedule request: %v\n", commandReq)

		scheduled_at, err := time.Parse(time.RFC3339, commandReq.Scheduled_At)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var mustBeExec_at *time.Time

		if commandReq.MustBeExecute_At != nil {
			parsedMustBeExec_at, err := time.Parse(time.RFC3339, *commandReq.MustBeExecute_At)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			mustBeExec_at = &parsedMustBeExec_at
		}

		taskID, err := s.insertTaskIntoDB(r.Context(), Task{Command: commandReq.Command,
			MustBeExecuteAt: mustBeExec_at,
			ScheduledAt:     &scheduled_at})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response := struct {
			Command          string  `json:"command"`
			MustBeExecute_At *string `json:"must_be_execute_at,omitempty"`
			Scheduled_At     *string `json:"scheduled_at"`
			TaskID           string  `json:"task_id"`
		}{
			Command:          commandReq.Command,
			MustBeExecute_At: formatPointedTime(mustBeExec_at),
			Scheduled_At:     formatPointedTime(&scheduled_at),
			TaskID:           taskID,
		}

		jsonResponse, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
	}
}

func (s *SchedulerServer) handleGetTask(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
		log.Println("Request cancelled (shutdown or client disconnected)")
	default:
		if r.Method != "GET" {
			http.Error(w, "only GET method is allowed", http.StatusMethodNotAllowed)
			return
		}

		task_id := r.URL.Query().Get("task_id")
		if task_id == "" {
			http.Error(w, "task_id is required", http.StatusBadRequest)
			return
		}

		var task Task
		err := s.dbPool.QueryRow(r.Context(), "SELECT * FROM TASKS WHERE id = $1", task_id).Scan(
			&task.ID,
			&task.Command,
			&task.MustBeExecuteAt,
			&task.ScheduledAt,
			&task.PickedAt,
			&task.StartedAt,
			&task.CompletedAt,
			&task.FailedAt,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response := struct {
			TaskID             string  `json:"task_id"`
			Command            string  `json:"command"`
			Must_be_execute_at *string `json:"must_be_execute_at,omitempty"`
			Scheduled_at       *string `json:"scheduled_at"`
			Picked_at          *string `json:"picked_at,omitempty"`
			Started_at         *string `json:"started_at,omitempty"`
			Completed_at       *string `json:"completed_at,omitempty"`
			Failed_at          *string `json:"failed_at,omitempty"`
		}{
			TaskID:             task.ID,
			Command:            task.Command,
			Must_be_execute_at: formatPointedTime(task.MustBeExecuteAt),
			Scheduled_at:       formatPointedTime(task.ScheduledAt),
			Picked_at:          formatPointedTime(task.PickedAt),
			Started_at:         formatPointedTime(task.StartedAt),
			Completed_at:       formatPointedTime(task.CompletedAt),
			Failed_at:          formatPointedTime(task.FailedAt),
		}

		jsonResponse, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		w.Write(jsonResponse)
	}
}

func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	sqlStatement := "INSERT INTO tasks (command, must_be_execute_at, scheduled_at) VALUES ($1, $2, $3) RETURNING id"

	var insertedId string

	err := s.dbPool.QueryRow(ctx, sqlStatement, task.Command, task.MustBeExecuteAt, task.ScheduledAt).Scan(&insertedId)
	if err != nil {
		return "", err
	}

	return insertedId, nil
}

func formatPointedTime(t *time.Time) *string {
	if t == nil {
		return nil
	}

	s := t.Format(time.RFC3339)
	return &s
}

func (s *SchedulerServer) awaitShutdown() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	<-stop

	s.Stop()
}

func (s *SchedulerServer) Stop(){
	stopOnce.Do(func() {
		if s.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			if err := s.httpServer.Shutdown(ctx); err != nil{
				log.Printf("Error while shutting down http server: %s", err)
			}
		}

		s.cancel()

		s.dbPool.Close()

		log.Println("Connect to database stopped")
	})
}

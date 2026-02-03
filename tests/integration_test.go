package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Krunis/DeffManager/packages/common"
	pb "github.com/Krunis/DeffManager/packages/grpcapi"
)

var cluster Cluster

func setup(countWorkers uint8) {
	cluster = Cluster{}
	cluster.LaunchCluster(":8081", ":8080", countWorkers)

}

func teardown() {
	cluster.StopCluster()
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestE2ESuccess(t *testing.T) {
	setup(4)
	defer teardown()

	postData := map[string]interface{}{
		"command":      "aooooooooa",
		"scheduled_at": "2025-12-24T22:34:00+05:30",
	}
	postDataBytes, _ := json.Marshal(postData)
	resp, err := http.Post("http://localhost:8081/schedule", "application/json", strings.NewReader(string(postDataBytes)))
	if err != nil {
		t.Fatalf("Failed to send POST request: %v", err)
	}
	defer resp.Body.Close()

	var postResponse map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&postResponse)
	taskID := postResponse["task_id"].(string)

	err = WaitForCondition(func() bool {
		getURL := "http://localhost:8081/status?task_id=" + url.QueryEscape(taskID)

		resp, err := http.Get(getURL)
		if err != nil {
			log.Fatalf("Failed to send GET request: %v", err)
		}
		defer resp.Body.Close()

		var getResponse map[string]any

		json.NewDecoder(resp.Body).Decode(&getResponse)

		_, pickedAtExists := getResponse["picked_at"]
		_, startedAtExists := getResponse["started_at"]
		_, completeddAtExists := getResponse["completed_at"]

		return pickedAtExists && startedAtExists && completeddAtExists

	}, 20*time.Second, time.Second)

	if err != nil {
		t.Fatalf("Response did not contain keys 'started_at', 'completed_at' or 'picked_at': %v", err)
	}
}

func TestWorkersNotAvailable(t *testing.T) {
	setup(2)
	defer teardown()

	conn, client := createTestClient(cluster.coordinatorAddress)
	for _, worker := range cluster.workers {
		stopServer(worker)
	}

	time.Sleep(time.Second * 5)

	err := WaitForCondition(func() bool {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		fmt.Println(err)
		return err != nil && err.Error() == "rpc error: code = Unknown desc = no workers available"
	}, 20*time.Second, common.DefaultHeartbeat)

	if err != nil {
		t.Fatalf("Coordinator did not clean up the workers within SLO. Error: %v", err)
	}
	conn.Close()
}

func TestCoordinatorFailoverForInactiveWorkers(t *testing.T) {
	setup(3)
	defer teardown()

	cluster.workers[0].Stop()
	cluster.workers[1].Stop()

	err := WaitForCondition(func() bool {
		cluster.coordinator.WorkerPoolMutex.Lock()
		countWorkers := len(cluster.coordinator.WorkerPool)
		cluster.coordinator.WorkerPoolMutex.Unlock()
		return countWorkers == 1
	}, time.Second*20, common.DefaultHeartbeat)
	if err != nil {
		t.Fatalf("Coordinator did not clean up for inactive workers")
	}

	conn, client := createTestClient(cluster.coordinatorAddress)

	for range 4 {
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		if err != nil {
			t.Logf("Failed to submit task: %v", err)
		}
	}

	err = WaitForCondition(func() bool {
		worker := cluster.workers[2]
		worker.ReceivedTasksMutex.Lock()
		if len(worker.ReceivedTasks) != 4 {
			worker.ReceivedTasksMutex.Unlock()
			return false
		}
		log.Println("Проверено")
		return true
	}, time.Second*10, time.Millisecond * 500)
	
	if err != nil{
		conn.Close()
		t.Fatalf("Coordinator not routing requests correctly after failover")
	}

	conn.Close()
}

func TestBalancingTasksOverWorkers(t *testing.T){
	setup(4)
	defer teardown()

	conn, client := createTestClient(cluster.coordinatorAddress)

	for range 8{
		_, err := client.SubmitTask(context.Background(), &pb.ClientTaskRequest{Data: "test"})
		if err != nil{
			t.Logf("Failed to submit task: %v", err)
		}
	}

	err := WaitForCondition(func() bool {
		for _, worker := range cluster.workers{
			worker.ReceivedTasksMutex.Lock()
			if len(worker.ReceivedTasks) != 2{
				worker.ReceivedTasksMutex.Unlock()
				return false
			}
			worker.ReceivedTasksMutex.Unlock()
		}
		return true
	}, time.Second * 5, time.Millisecond * 500)
	
	if err != nil{
		for idx, worker := range cluster.workers{
			worker.ReceivedTasksMutex.Lock()
			t.Logf("Worker %d has %d tasks in its log", idx, len(worker.ReceivedTasks))
			worker.ReceivedTasksMutex.Unlock()
		}
		t.Fatalf("Coordinator is dont use round-robin to execute tasks over worker pool")
	}

	conn.Close()
}

func WaitForCondition(condition func() bool, timeout time.Duration, retryInterval time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if condition() {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout exceeded")
		}
	}
}

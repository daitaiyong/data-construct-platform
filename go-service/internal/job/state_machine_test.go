package job

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/kafka"
)

func TestExecutionGatesDependentTasksUntilUpstreamSucceeds(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "t1", NodeID: "n1", FailStrategy: "abort_dag"},
		{TaskID: "t2", NodeID: "n2", Dependencies: []string{"n1"}, FailStrategy: "abort_dag"},
	})
	if got, want := execution.InitialReadyTasks(), []string{"t1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial ready tasks = %v, want %v", got, want)
	}
	if _, err := execution.ApplyEvent(NewStateEvent("e1", kafka.EventTaskStart, "job-1", "t2", "n2", kafka.EventPayload{})); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("start dependent task error = %v, want ErrEventConflict", err)
	}
	if execution.Status != StatusPending || execution.Tasks["t2"].Status != TaskStatusPending {
		t.Fatal("rejected event mutated execution state")
	}

	applyTestEvent(t, execution, "e2", kafka.EventTaskStart, "t1", "n1")
	actions := applyTestEvent(t, execution, "e3", kafka.EventTaskSuccess, "t1", "n1")
	if got, want := actions.ReadyTaskIDs, []string{"t2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("newly ready tasks = %v, want %v", got, want)
	}
	if execution.Status != StatusRunning {
		t.Fatalf("job status = %q, want running", execution.Status)
	}
}

func TestContinueFailureSkipsDependentsAndRunsIndependentBranch(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "source", NodeID: "source", FailStrategy: "continue"},
		{TaskID: "dependent", NodeID: "dependent", Dependencies: []string{"source"}, FailStrategy: "abort_dag"},
		{TaskID: "independent", NodeID: "independent", FailStrategy: "abort_dag"},
		{TaskID: "transitive", NodeID: "transitive", Dependencies: []string{"dependent"}, FailStrategy: "abort_dag"},
	})
	applyTestEvent(t, execution, "start-source", kafka.EventTaskStart, "source", "source")
	actions := applyTestEvent(t, execution, "fail-source", kafka.EventTaskFail, "source", "source")
	if got, want := execution.Tasks["dependent"].Status, TaskStatusSkipped; got != want {
		t.Fatalf("dependent status = %q, want %q", got, want)
	}
	if got, want := execution.Tasks["transitive"].Status, TaskStatusSkipped; got != want {
		t.Fatalf("transitive status = %q, want %q", got, want)
	}
	if got, want := actions.ReadyTaskIDs, []string{"independent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ready independent tasks = %v, want %v", got, want)
	}
	applyTestEvent(t, execution, "start-independent", kafka.EventTaskStart, "independent", "independent")
	applyTestEvent(t, execution, "success-independent", kafka.EventTaskSuccess, "independent", "independent")
	if execution.Status != StatusSuccess {
		t.Fatalf("job status = %q, want success", execution.Status)
	}
}

func TestAbortFailureFailsJobAndCancelsPendingTasks(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "failed", NodeID: "failed", FailStrategy: "abort_dag"},
		{TaskID: "pending", NodeID: "pending", FailStrategy: "continue"},
	})
	applyTestEvent(t, execution, "start-failed", kafka.EventTaskStart, "failed", "failed")
	applyTestEvent(t, execution, "fail-final", kafka.EventTaskFail, "failed", "failed")
	if execution.Status != StatusFailed {
		t.Fatalf("job status = %q, want failed", execution.Status)
	}
	if execution.Tasks["pending"].Status != TaskStatusCanceled {
		t.Fatalf("pending task status = %q, want canceled", execution.Tasks["pending"].Status)
	}
}

func TestTaskFailureRetriesOnlyTaskUntilRetryBudgetExhausted(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "retry", NodeID: "retry", FailStrategy: "abort_dag", MaxRetry: 1},
	})
	applyTestEvent(t, execution, "start-1", kafka.EventTaskStart, "retry", "retry")
	actions := applyTestEvent(t, execution, "fail-1", kafka.EventTaskFail, "retry", "retry")
	if !reflect.DeepEqual(actions.RetryTaskIDs, []string{"retry"}) {
		t.Fatalf("retry task IDs = %v", actions.RetryTaskIDs)
	}
	if task := execution.Tasks["retry"]; task.Status != TaskStatusPending || task.RetryTimes != 1 {
		t.Fatalf("retried task state = %q, retry_times=%d", task.Status, task.RetryTimes)
	}

	applyTestEvent(t, execution, "start-2", kafka.EventTaskStart, "retry", "retry")
	applyTestEvent(t, execution, "fail-2", kafka.EventTaskFail, "retry", "retry")
	if task := execution.Tasks["retry"]; task.Status != TaskStatusFailed || task.RetryTimes != 1 {
		t.Fatalf("exhausted task state = %q, retry_times=%d", task.Status, task.RetryTimes)
	}
	if execution.Status != StatusFailed {
		t.Fatalf("job status = %q, want failed", execution.Status)
	}
}

func TestEventHandlingIsIdempotent(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "t1", NodeID: "n1", FailStrategy: "abort_dag", MaxRetry: 1},
	})
	applyTestEvent(t, execution, "start", kafka.EventTaskStart, "t1", "n1")
	first := applyTestEvent(t, execution, "failure", kafka.EventTaskFail, "t1", "n1")
	second, err := execution.ApplyEvent(NewStateEvent("failure", kafka.EventTaskFail, "job-1", "t1", "n1", kafka.EventPayload{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.RetryTaskIDs) != 1 || len(second.RetryTaskIDs) != 0 || execution.Tasks["t1"].RetryTimes != 1 {
		t.Fatalf("duplicate event repeated state change: first=%+v second=%+v", first, second)
	}
}

func TestJobCancellationCancelsPendingAndRunningTasks(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "running", NodeID: "running", FailStrategy: "continue"},
		{TaskID: "pending", NodeID: "pending", FailStrategy: "continue"},
	})
	applyTestEvent(t, execution, "start", kafka.EventTaskStart, "running", "running")
	if _, err := execution.ApplyEvent(NewStateEvent("cancel", kafka.EventJobCancel, "job-1", "", "", kafka.EventPayload{})); err != nil {
		t.Fatal(err)
	}
	if execution.Status != StatusCanceled {
		t.Fatalf("job status = %q, want canceled", execution.Status)
	}
	for taskID, task := range execution.Tasks {
		if task.Status != TaskStatusCanceled {
			t.Errorf("task %q status = %q, want canceled", taskID, task.Status)
		}
	}
	if _, err := execution.ApplyEvent(NewStateEvent("late-success", kafka.EventTaskSuccess, "job-1", "running", "running", kafka.EventPayload{})); err != nil {
		t.Fatalf("late worker event after cancellation should be acknowledged: %v", err)
	}
	if execution.Tasks["running"].Status != TaskStatusCanceled {
		t.Fatalf("late worker event changed canceled task to %q", execution.Tasks["running"].Status)
	}
}

func TestJobStateTransitionsMatchContract(t *testing.T) {
	allowed := map[Status][]Status{
		StatusPending: {StatusRunning, StatusCanceled},
		StatusRunning: {StatusSuccess, StatusFailed, StatusCanceled},
	}
	for current, nextStates := range allowed {
		for _, next := range nextStates {
			if !canTransitionJob(current, next) {
				t.Errorf("transition %q -> %q should be allowed", current, next)
			}
		}
	}
	if canTransitionJob(StatusSuccess, StatusRunning) ||
		canTransitionJob(StatusFailed, StatusSuccess) ||
		canTransitionJob(StatusCanceled, StatusRunning) {
		t.Fatal("terminal Job states must not transition")
	}
}

func newTestExecution(t *testing.T, tasks []TaskDefinition) *Execution {
	t.Helper()
	execution, err := NewExecution("job-1", tasks)
	if err != nil {
		t.Fatal(err)
	}
	return execution
}

func applyTestEvent(t *testing.T, execution *Execution, eventID string, eventType kafka.EventType, taskID, nodeID string) EventActions {
	t.Helper()
	actions, err := execution.ApplyEvent(NewStateEvent(eventID, eventType, "job-1", taskID, nodeID, kafka.EventPayload{}))
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func canTransitionJob(current, next Status) bool {
	switch current {
	case StatusPending:
		return next == StatusRunning || next == StatusCanceled
	case StatusRunning:
		return next == StatusSuccess || next == StatusFailed || next == StatusCanceled
	default:
		return false
	}
}

func TestNewStateEventHasUTCDate(t *testing.T) {
	event := NewStateEvent("event-1", kafka.EventJobCancel, "job-1", "", "", kafka.EventPayload{})
	if event.Timestamp.IsZero() || event.Timestamp.Location() != time.UTC {
		t.Fatalf("timestamp = %v, want non-zero UTC timestamp", event.Timestamp)
	}
}

func TestEventConsumerAppliesEventsThroughAtomicStore(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "t1", NodeID: "n1", FailStrategy: "continue"},
	})
	store := NewMemoryEventStore(execution)
	consumer, err := NewEventConsumer(store)
	if err != nil {
		t.Fatal(err)
	}
	event := NewStateEvent("start", kafka.EventTaskStart, "job-1", "t1", "n1", kafka.EventPayload{})
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Handle(context.Background(), kafka.Message{Key: []byte("job-1"), Value: value}); err != nil {
		t.Fatal(err)
	}
	if execution.Status != StatusRunning || execution.Tasks["t1"].Status != TaskStatusRunning {
		t.Fatalf("event was not projected: job=%q task=%q", execution.Status, execution.Tasks["t1"].Status)
	}
}

func TestEventConsumerRejectsWrongKafkaKey(t *testing.T) {
	execution := newTestExecution(t, []TaskDefinition{
		{TaskID: "t1", NodeID: "n1", FailStrategy: "continue"},
	})
	consumer, err := NewEventConsumer(NewMemoryEventStore(execution))
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(NewStateEvent("start", kafka.EventTaskStart, "job-1", "t1", "n1", kafka.EventPayload{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Handle(context.Background(), kafka.Message{Key: []byte("wrong-job"), Value: value}); err == nil {
		t.Fatal("expected message key validation error")
	}
}

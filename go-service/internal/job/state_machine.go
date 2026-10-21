package job

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/kafka"
)

var (
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrJobTerminal       = errors.New("job is already terminal")
	ErrEventConflict     = errors.New("event does not match execution state")
)

type TaskDefinition struct {
	TaskID       string
	NodeID       string
	Dependencies []string
	FailStrategy string
	RetryTimes   uint32
	MaxRetry     uint32
}

type TaskExecution struct {
	TaskDefinition
	Status   TaskStatus
	ErrorMsg string
}

type Execution struct {
	mu           sync.Mutex
	JobID        string
	Status       Status
	Tasks        map[string]*TaskExecution
	seenEventIDs map[string]struct{}
}

type EventActions struct {
	RetryTaskIDs []string
	ReadyTaskIDs []string
}

func NewExecution(jobID string, definitions []TaskDefinition) (*Execution, error) {
	if jobID == "" {
		return nil, errors.New("job_id must not be empty")
	}
	if len(definitions) == 0 {
		return nil, errors.New("execution must contain at least one task")
	}
	execution := &Execution{
		JobID:        jobID,
		Status:       StatusPending,
		Tasks:        make(map[string]*TaskExecution, len(definitions)),
		seenEventIDs: make(map[string]struct{}),
	}
	nodeToTask := make(map[string]string, len(definitions))
	for _, definition := range definitions {
		if definition.TaskID == "" || definition.NodeID == "" {
			return nil, errors.New("task_id and node_id must not be empty")
		}
		if _, exists := execution.Tasks[definition.TaskID]; exists {
			return nil, fmt.Errorf("duplicate task_id %q", definition.TaskID)
		}
		if _, exists := nodeToTask[definition.NodeID]; exists {
			return nil, fmt.Errorf("duplicate node_id %q", definition.NodeID)
		}
		if definition.FailStrategy != "abort_dag" && definition.FailStrategy != "continue" {
			return nil, fmt.Errorf("task %q has invalid fail_strategy %q", definition.TaskID, definition.FailStrategy)
		}
		if definition.RetryTimes > definition.MaxRetry {
			return nil, fmt.Errorf("task %q retry_times exceeds max_retry", definition.TaskID)
		}
		execution.Tasks[definition.TaskID] = &TaskExecution{
			TaskDefinition: definition,
			Status:         TaskStatusPending,
		}
		nodeToTask[definition.NodeID] = definition.TaskID
	}
	for _, task := range execution.Tasks {
		seenDependencies := make(map[string]struct{}, len(task.Dependencies))
		for _, nodeID := range task.Dependencies {
			if _, exists := nodeToTask[nodeID]; !exists {
				return nil, fmt.Errorf("task %q depends on unknown node %q", task.TaskID, nodeID)
			}
			if nodeID == task.NodeID {
				return nil, fmt.Errorf("task %q cannot depend on itself", task.TaskID)
			}
			if _, exists := seenDependencies[nodeID]; exists {
				return nil, fmt.Errorf("task %q has duplicate dependency %q", task.TaskID, nodeID)
			}
			seenDependencies[nodeID] = struct{}{}
		}
	}
	if hasDependencyCycle(execution.Tasks) {
		return nil, errors.New("execution dependencies contain a cycle")
	}
	return execution, nil
}

func (e *Execution) InitialReadyTasks() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.readyTasksLocked()
}

func (e *Execution) ApplyEvent(event kafka.EventMessage) (EventActions, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if event.JobID != e.JobID {
		return EventActions{}, fmt.Errorf("%w: event job_id %q does not match %q", ErrEventConflict, event.JobID, e.JobID)
	}
	if _, exists := e.seenEventIDs[event.EventID]; exists {
		return EventActions{}, nil
	}
	if err := event.Validate(); err != nil {
		return EventActions{}, err
	}

	var actions EventActions
	switch event.EventType {
	case kafka.EventTaskStart, kafka.EventTaskSuccess, kafka.EventTaskFail:
		task, exists := e.Tasks[event.TaskID]
		if !exists || task.NodeID != event.NodeID {
			return EventActions{}, fmt.Errorf("%w: unknown task/node pair %q/%q", ErrEventConflict, event.TaskID, event.NodeID)
		}
		if isTerminalJob(e.Status) && task.Status == TaskStatusCanceled {
			e.seenEventIDs[event.EventID] = struct{}{}
			return EventActions{}, nil
		}
		if isTerminalJob(e.Status) && !(e.Status == StatusFailed && task.Status == TaskStatusRunning &&
			(event.EventType == kafka.EventTaskSuccess || event.EventType == kafka.EventTaskFail)) {
			return EventActions{}, ErrJobTerminal
		}
		switch event.EventType {
		case kafka.EventTaskStart:
			if isTerminalJob(e.Status) {
				return EventActions{}, ErrJobTerminal
			}
			if !e.dependenciesSucceededLocked(task) {
				return EventActions{}, fmt.Errorf("%w: task %q has unfinished dependencies", ErrEventConflict, task.TaskID)
			}
			if err := transitionTask(task, TaskStatusRunning); err != nil {
				return EventActions{}, err
			}
			if e.Status == StatusPending {
				if err := transitionJob(e, StatusRunning); err != nil {
					return EventActions{}, err
				}
			}
		case kafka.EventTaskSuccess:
			if e.Status != StatusRunning {
				return EventActions{}, fmt.Errorf("%w: task_success requires a running job", ErrInvalidTransition)
			}
			if err := transitionTask(task, TaskStatusSuccess); err != nil {
				return EventActions{}, err
			}
			actions.ReadyTaskIDs = e.readyTasksLocked()
			if err := e.finishIfCompleteLocked(); err != nil {
				return EventActions{}, err
			}
		case kafka.EventTaskFail:
			if e.Status != StatusRunning {
				return EventActions{}, fmt.Errorf("%w: task_fail requires a running job", ErrInvalidTransition)
			}
			if err := transitionTask(task, TaskStatusFailed); err != nil {
				return EventActions{}, err
			}
			task.ErrorMsg = event.Payload.ErrorMessage
			if e.Status == StatusFailed {
				break
			}
			if task.RetryTimes < task.MaxRetry {
				task.RetryTimes++
				if err := transitionTask(task, TaskStatusPending); err != nil {
					return EventActions{}, err
				}
				actions.RetryTaskIDs = []string{task.TaskID}
				break
			}
			if task.FailStrategy == "abort_dag" {
				if err := transitionJob(e, StatusFailed); err != nil {
					return EventActions{}, err
				}
				e.cancelPendingTasksLocked()
			} else {
				e.skipBlockedTasksLocked()
				actions.ReadyTaskIDs = e.readyTasksLocked()
				if err := e.finishIfCompleteLocked(); err != nil {
					return EventActions{}, err
				}
			}
		}
	case kafka.EventJobSuccess:
		if e.Status == StatusSuccess {
			break
		}
		if e.Status != StatusRunning || !e.allTasksTerminalLocked() || e.hasAbortFailureLocked() {
			return EventActions{}, fmt.Errorf("%w: job cannot succeed while tasks remain or an abort failure exists", ErrEventConflict)
		}
		if err := transitionJob(e, StatusSuccess); err != nil {
			return EventActions{}, err
		}
	case kafka.EventJobFail:
		if e.Status == StatusFailed {
			break
		}
		if e.Status != StatusRunning {
			return EventActions{}, fmt.Errorf("%w: job_fail requires a running job", ErrInvalidTransition)
		}
		if err := transitionJob(e, StatusFailed); err != nil {
			return EventActions{}, err
		}
		e.cancelPendingTasksLocked()
	case kafka.EventJobCancel:
		if e.Status == StatusCanceled {
			break
		}
		if e.Status != StatusPending && e.Status != StatusRunning {
			return EventActions{}, fmt.Errorf("%w: cannot cancel job in state %q", ErrInvalidTransition, e.Status)
		}
		if err := transitionJob(e, StatusCanceled); err != nil {
			return EventActions{}, err
		}
		e.cancelUnfinishedTasksLocked()
	default:
		return EventActions{}, fmt.Errorf("%w: unsupported event type %q", ErrEventConflict, event.EventType)
	}
	e.seenEventIDs[event.EventID] = struct{}{}
	return actions, nil
}

func (e *Execution) readyTasksLocked() []string {
	if e.Status != StatusPending && e.Status != StatusRunning {
		return nil
	}
	ready := make([]string, 0)
	for taskID, task := range e.Tasks {
		if task.Status == TaskStatusPending && e.dependenciesSucceededLocked(task) {
			ready = append(ready, taskID)
		}
	}
	sort.Strings(ready)
	return ready
}

func (e *Execution) dependenciesSucceededLocked(task *TaskExecution) bool {
	for _, dependencyNode := range task.Dependencies {
		dependency := e.taskByNodeLocked(dependencyNode)
		if dependency == nil || dependency.Status != TaskStatusSuccess {
			return false
		}
	}
	return true
}

func (e *Execution) skipBlockedTasksLocked() {
	changed := true
	for changed {
		changed = false
		for _, task := range e.Tasks {
			if task.Status != TaskStatusPending {
				continue
			}
			for _, dependencyNode := range task.Dependencies {
				dependency := e.taskByNodeLocked(dependencyNode)
				if dependency == nil || dependency.Status == TaskStatusFailed || dependency.Status == TaskStatusSkipped || dependency.Status == TaskStatusCanceled {
					task.Status = TaskStatusSkipped
					changed = true
					break
				}
			}
		}
	}
}

func (e *Execution) cancelPendingTasksLocked() {
	for _, task := range e.Tasks {
		if task.Status == TaskStatusPending {
			task.Status = TaskStatusCanceled
		}
	}
}

func (e *Execution) cancelUnfinishedTasksLocked() {
	for _, task := range e.Tasks {
		if task.Status == TaskStatusPending || task.Status == TaskStatusRunning {
			task.Status = TaskStatusCanceled
		}
	}
}

func (e *Execution) finishIfCompleteLocked() error {
	if e.Status == StatusRunning && e.allTasksTerminalLocked() && !e.hasAbortFailureLocked() {
		return transitionJob(e, StatusSuccess)
	}
	return nil
}

func (e *Execution) allTasksTerminalLocked() bool {
	for _, task := range e.Tasks {
		if task.Status == TaskStatusPending || task.Status == TaskStatusRunning {
			return false
		}
	}
	return true
}

func (e *Execution) hasAbortFailureLocked() bool {
	for _, task := range e.Tasks {
		if task.Status == TaskStatusFailed && task.FailStrategy == "abort_dag" {
			return true
		}
	}
	return false
}

func (e *Execution) taskByNodeLocked(nodeID string) *TaskExecution {
	for _, task := range e.Tasks {
		if task.NodeID == nodeID {
			return task
		}
	}
	return nil
}

func transitionTask(task *TaskExecution, next TaskStatus) error {
	allowed := false
	switch task.Status {
	case TaskStatusPending:
		allowed = next == TaskStatusRunning || next == TaskStatusCanceled || next == TaskStatusSkipped
	case TaskStatusRunning:
		allowed = next == TaskStatusSuccess || next == TaskStatusFailed || next == TaskStatusCanceled
	case TaskStatusFailed:
		allowed = next == TaskStatusPending
	}
	if !allowed {
		return fmt.Errorf("%w: task %q cannot transition from %q to %q", ErrInvalidTransition, task.TaskID, task.Status, next)
	}
	task.Status = next
	return nil
}

func isTerminalJob(status Status) bool {
	return status == StatusSuccess || status == StatusFailed || status == StatusCanceled
}

func transitionJob(execution *Execution, next Status) error {
	allowed := execution.Status == StatusPending && (next == StatusRunning || next == StatusCanceled) ||
		execution.Status == StatusRunning && (next == StatusSuccess || next == StatusFailed || next == StatusCanceled)
	if !allowed {
		return fmt.Errorf("%w: job %q cannot transition from %q to %q", ErrInvalidTransition, execution.JobID, execution.Status, next)
	}
	execution.Status = next
	return nil
}

func hasDependencyCycle(tasks map[string]*TaskExecution) bool {
	state := make(map[string]uint8, len(tasks))
	var visit func(*TaskExecution) bool
	visit = func(task *TaskExecution) bool {
		switch state[task.NodeID] {
		case 1:
			return true
		case 2:
			return false
		}
		state[task.NodeID] = 1
		for _, dependencyNode := range task.Dependencies {
			if dependency := nodeTask(tasks, dependencyNode); dependency != nil && visit(dependency) {
				return true
			}
		}
		state[task.NodeID] = 2
		return false
	}
	for _, task := range tasks {
		if visit(task) {
			return true
		}
	}
	return false
}

func nodeTask(tasks map[string]*TaskExecution, nodeID string) *TaskExecution {
	for _, task := range tasks {
		if task.NodeID == nodeID {
			return task
		}
	}
	return nil
}

func NewStateEvent(eventID string, eventType kafka.EventType, jobID, taskID, nodeID string, payload kafka.EventPayload) kafka.EventMessage {
	return kafka.EventMessage{
		EventID:   eventID,
		EventType: eventType,
		JobID:     jobID,
		TaskID:    taskID,
		NodeID:    nodeID,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}
}

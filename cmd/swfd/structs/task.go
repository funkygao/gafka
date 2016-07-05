package structs

type Task struct {
}

type Decision struct {
}

type HistoryEvent struct {
}

type WorkflowExecution struct {
	workflowId string // 1-256 in len
	runId      string // 1-64 in len
}

type WorkflowType struct {
	name    string // 1-256 in len
	version string // 1-64 in len
}

type decisionType int

type historyEventType int

const (
	ActivityTaskCancelRequested historyEventType = iota
	ActivityTaskCanceled
	ActivityTaskCompleted
	ActivityTaskFailed
	ActivityTaskScheduled
	ActivityTaskStarted
	ActivityTaskTimedOut
	CancelTimerFailed
	CancelWorkflowExecutionFailed
	ChildWorkflowExecutionCanceled
	DecisionTaskScheduled
	DecisionTaskStarted
	// ...
)

const (
	CancelTimer decisionType = iota
	CancelWorkflowExecution
	CompleteWorkflowExecution
	ContinueAsNewWorkflowExecution
	FailWorkflowExecution
	RecordMarker
	RequestCancelActivityTask
	RequestCancelExternalWorkflowExecution
	ScheduleActivityTask
	SignalExternalWorkflowExecution
	StartChildWorkflowExecution
	StartTimer
)

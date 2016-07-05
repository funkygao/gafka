package swf

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (this *apiServer) mainEntryPoint(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// X-Amz-Target: SimpleWorkflowService.StartWorkflowExecution
	// X-Amzn-Authorization: AWS3 AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE,Algorithm=HmacSHA256,SignedHeaders=Host;X-Amz-Date;X-Amz-Target;Content-Encoding,Signature=aYxuqLX+TO91kPVg+jh+aA8PWxQazQRN2+SZUGdOgU0=

	api := r.Header.Get("X-Swf-Api")
	switch api {
	case opStartWorkflowExecution:
	case opTerminateWorkflowExecution:

	}
}

func (this *apiServer) startWorkflowExecution(queue string, workflowId string, input string) {

}

// When a decider schedules an activity task, it provides the data (which you determine) that the activity worker needs to perform the activity task.
// Amazon SWF inserts this data into the activity task before sending it to the activity worker.
//
// The execution state for a workflow execution is stored in its workflow history.
// There can be only one decision task open at any time for a given workflow execution.
// Every time a state change occurs for a workflow execution, Amazon SWF schedules a decision task.

/*
RegisterDomain(name, description, workflowExecutionRetentionPeriodInDays string)

RegisterWorkflowType(domain, name, version, description, defaultTaskList string)

RegisterActivityType(domain, name, version, description, defaultTaskList string)

// workflowId unique across StartWorkflowExecution
StartWorkflowExecution(domain, taskList, workflowType, workflowId, input string, tagList []string) (runId string)

PollForDecisionTask(domain, taskList, identity string, maximumPageSize int, reverseOrder bool) (events []HistoryEvent, previousStartedEventId int64, startedEventId int64, taskToken string, we WorkflowExecution)
RespondDecisionTaskCompleted(decisions []Decision, executionContext string, taskToken string)

PollForActivityTask(domain, taskList, identity string) (activityId string, activityType, input, startedEventId, taskToken, runId, workflowId string)
RespondActivityTaskCompleted(result, taskToken string)

*/

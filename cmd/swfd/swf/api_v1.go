package swf

func (this *Swf) setupApis() {
	m := this.Middleware

	if this.apiServer != nil {
		this.apiServer.Router().GET("/alive", m(this.apiServer.checkAliveHandler))

		// learned from amazon:
		// all apis are POST methods because we need json as input params
		// endpoint e,g. swf.us-east-1.amazonaws.com
		const opCountClosedWorkflowExecutions = "CountClosedWorkflowExecutions"
		const opCountOpenWorkflowExecutions = "CountOpenWorkflowExecutions"
		const opCountPendingActivityTasks = "CountPendingActivityTasks"
		const opCountPendingDecisionTasks = "CountPendingDecisionTasks"
		const opDeprecateActivityType = "DeprecateActivityType"
		const opDeprecateDomain = "DeprecateDomain"
		const opDeprecateWorkflowType = "DeprecateWorkflowType"
		const opDescribeActivityType = "DescribeActivityType"
		const opDescribeDomain = "DescribeDomain"
		const opDescribeWorkflowExecution = "DescribeWorkflowExecution"
		const opDescribeWorkflowType = "DescribeWorkflowType"
		const opGetWorkflowExecutionHistory = "GetWorkflowExecutionHistory"
		const opListActivityTypes = "ListActivityTypes"
		const opListClosedWorkflowExecutions = "ListClosedWorkflowExecutions"
		const opListDomains = "ListDomains"
		const opListOpenWorkflowExecutions = "ListOpenWorkflowExecutions"
		const opListWorkflowTypes = "ListWorkflowTypes"

		const opPollForActivityTask = "PollForActivityTask"
		const opPollForDecisionTask = "PollForDecisionTask"

		const opSignalWorkflowExecution = "SignalWorkflowExecution"
		const opStartWorkflowExecution = "StartWorkflowExecution"
		const opTerminateWorkflowExecution = "TerminateWorkflowExecution"

		const opRecordActivityTaskHeartbeat = "RecordActivityTaskHeartbeat"

		const opRegisterActivityType = "RegisterActivityType"
		const opRegisterDomain = "RegisterDomain"
		const opRegisterWorkflowType = "RegisterWorkflowType"

		const opRequestCancelWorkflowExecution = "RequestCancelWorkflowExecution"
		const opRespondActivityTaskCanceled = "RespondActivityTaskCanceled"
		const opRespondActivityTaskCompleted = "RespondActivityTaskCompleted"
		const opRespondActivityTaskFailed = "RespondActivityTaskFailed"
		const opRespondDecisionTaskCompleted = "RespondDecisionTaskCompleted"
	}

}

# amazon simple work flow/swf

SWF coordinates the flow of synchronous or asynchronous tasks (logical application steps) so that you can focus on your business and your application instead of having to worry about the infrastructure.

### Terms

- A Workflow is the automation of a business process.
- A Domain is a collection of related Workflows.
- Actions are the individual tasks undertaken to carry out a Workflow.
- Activity Workers are the pieces of code that actually implement the tasks. 
- A Decider implements a Workflow’s coordination logic.

### API

#### WorkFlow Registration

- RegisterDomain
- RegisterWorkflowType
- RegisterActivityType

#### Deciders and Workers

- PollForDecisionTask
- RespondDecisionTaskCompleted
- PollForActivityTask 
- RespondActivityTaskCompleted

#### Start a WorkFlow

- StartWorkflowExecution


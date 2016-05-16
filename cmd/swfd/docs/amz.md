# swf

### Task List

Amazon SWF stores the activity tasks in the workflow history and schedules them for execution by placing them on the activity task list. The activity workers poll the task list and execute the tasks.

### Workflow History

The workflow history is a detailed, complete, and consistent record of every event that occurred since the workflow execution started.

The workflow history contains every event that causes the execution state of the workflow execution to change, such as scheduled and completed activities, task timeouts, and signals.

Operations that do not change the state of the workflow execution do not typically appear in the workflow history. For example, the workflow history does not show poll attempts or the use of visibility operations.

Amazon SWF stores the complete history of all workflow executions for a configurable number of days after the execution closes. This period, which is known as the workflow history retention period, is specified when you register a Domain for your workflow. 

#### Benifits

- It enables applications to be stateless
- The history provides a detailed audit trail that you can use to monitor


    POST / HTTP/1.1
    Host: swf.us-east-1.amazonaws.com
    Keep-Alive: 115
    Connection: keep-alive
    Content-Type: application/x-amz-json-1.0
    X-Requested-With: XMLHttpRequest
    X-Amz-Date: Mon, 16 Jan 2012 03:44:00 GMT
    X-Amz-Target: SimpleWorkflowService.GetWorkflowExecutionHistory
    Content-Encoding: amz-1.0
    X-Amzn-Authorization: AWS3 AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE,Algorithm=HmacSHA256,SignedHeaders=Host;X-Amz-Date;X-Amz-Target;Content-Encoding,Signature=90GENeUWJbEAMWuVI0dcWatHjltMWddXfLjl0MbNOzM=
    Content-Length: 175
    Pragma: no-cache
    Cache-Control: no-cache
    
    {
      "maximumPageSize": 10,
      "domain": "867530901",
      "execution": {
        "workflowId": "20110927-T-1",
        "runId": "d29e60b5-fa71-4276-a4be-948b0adcd20b"
      },
      "reverseOrder": true
    }


     HTTP/1.1 200 OK
     Content-Length: 837
     Content-Type: application/json
     x-amzn-RequestId: b48fb6b5-3ff5-11e1-a23a-99d60383ae71

    {
        "activityId": "verification-27",
        "activityType": {
            "name": "activityVerify",
            "version": "1.0"
        },
        "input": "5634-0056-4367-0923,12/12,437",
        "startedEventId": 11,
        "taskToken": "AAAAKgAAAAEAAAAAAAAAAX9p3pcp3857oLXFUuwdxRU5/zmn9f40XaMF7VohAH4jOtjXpZu7GdOzEi0b3cWYHbG5b5dpdcTXHUDPVMHXiUxCgr+Nc/wUW9016W4YxJGs/jmxzPln8qLftU+SW135Q0UuKp5XRGoRTJp3tbHn2pY1vC8gDB/K69J6q668U1pd4Cd9o43//lGgOIjN0/Ihg+DO+83HNcOuVEQMM28kNMXf7yePh31M4dMKJwQaQZG13huJXDwzJOoZQz+XFuqFly+lPnCE4XvsnhfAvTsh50EtNDEtQzPCFJoUeld9g64V/FS/39PHL3M93PBUuroPyHuCwHsNC6fZ7gM/XOKmW4kKnXPoQweEUkFV/J6E6+M1reBO7nJADTrLSnajg6MY/viWsEYmMw/DS5FlquFaDIhFkLhWUWN+V2KqiKS23GYwpzgZ7fgcWHQF2NLEY3zrjam4LW/UW5VLCyM3FpVD3erCTi9IvUgslPzyVGuWNAoTmgJEWvimgwiHxJMxxc9JBDR390iMmImxVl3eeSDUWx8reQltiviadPDjyRmVhYP8",
        "workflowExecution": {
            "runId": "cfa2bd33-31b0-4b75-b131-255bb0d97b3f",
            "workflowId": "20110927-T-1"
        }
    }

# KAA IoT


### Architecture

- thrift
- avro
- zk
- cassandra/mongodb

### Schema

Defined in Server and built into endpoints with versions.

- endpoint profile schema
- notification schema
- event class schema

### Server

org.kaaproject.kaa.server.node.KaaNodeApplication

- REST interfaces
- log appenders for integration with analytics
  - Avro log schema

### Components

#### Topic based notification

Each topic created in Kaa can be assigned to one or more endpoint groups (global by default).

       message
Server -------> Endpoint

- system type notification
- user type notification

    

    struct Notification {
        1: id appId  // mongodb ObjectId
        2: shared.Integer appSeqNumber
        3: id groupId
        4: shared.Integer groupSeqNumber
        5: id profileFilterId
        6: shared.Integer profileFilterSeqNumber
        7: id configurationId
        8: shared.Integer configurationSeqNumber
        9: id notificationId
        10: id topicId
        11: Operation op
        12: id appenderId
        13: string userVerifierToken
    }



#### Events

The Kaa Event subsystem enables generation of events on endpoints in near real-time fashion, handling those events on a Kaa server, and dispatching them to other endpoints that belong to the same user (potentially, across different applications).

Souce -> Sink

Events can be sent to a single endpoint (unicast traffic) or to all the event sink endpoints of the given user (multicast traffic).
In case of a multicast event, the Kaa server relays the event to all endpoints registered as the corresponding EC sinks during the ECF mapping. 

If the user's endpoints are distributed over multiple Operation servers, the event is sent to all these Operation servers. Until being expired, thew event remains deliverable for the endpoints that were offline at the moment of the event generation.

Message type:

- ROUTE_UPDATE
- USER_ROUTE_INFO
- EVENT
- ENDPOINT_ROUTE_UPDATE
- ENDPOINT_STATE_UPDATE


# es

ElasticSearch console

### Usage

    $es
    ElasticSearch Console
    
    usage: es [--version] [--help] <command> [<args>]
    
    Available commands are:
        aliases       Currently configured aliases to indices
        allocation    Display #shards and disk space used by data node
        clusters      ElasticSearch clusters
        count         Document count of the entire cluster
        health        Health of cluster
        indices       List indices
        merge         Visualize segments merge process
        nodes         Display nodes of cluster
        pending       Pending tasks
        plugins       Provides a view per node of running plugins
        segments      Display low level segments in shards
        shards        Detailed view of what nodes contain which shards
        threads       Show cluster wide thread pool per node
        top           Unix “top” like utility for ElasticSearch

# gafka
Simplified CLI kafka clusters management tool powered by golang.

### Install

    go get github.com/funkygao/gafka
    sudo cp $GOPATH/src/github.com/funkygao/gafka/etc/gafka.cf /etc

### Configuration

    /etc/gafka.cf
    {
        zones: [
            {
                name: "sit"
                zk: "10.77.144.87:10181,10.77.144.88:10181,10.77.144.89:10181"
            }
            {
                name: "test"
                zk: "10.77.144.101:10181,10.77.144.132:10181,10.77.144.182:10181"
            }
            {
                name: "prod"
                zk: "10.209.33.69:2181,10.209.37.19:2181,10.209.37.68:2181"
            }
        ]
    
        loglevel: "debug"
        kafka_home: "/opt/kafka_2.10-0.8.1.1"
    }

### Usage

    gafka


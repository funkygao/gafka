# boot servers and agents

### zk1:10.209.33.69
consul agent -server -bootstrap-expect 3 -data-dir /var/consul -dc cd -ui-dir /var/consul/ui 

### zk2 and zk3
consul agent -server -bootstrap-expect 3 -data-dir /var/consul -dc cd
consul join 10.209.33.69

### on each agent
nohup consul agent -data-dir /var/consul -dc=cd &
consul join 10.209.33.69

# register service
curl -X PUT -d '{"Datacenter": "cd", "Node": "kafka1", "Address": "10.209.37.39","Service": {"Service": "kafka", "tags": ["goods","v1"], "Port": 10111}}' http://127.0.0.1:8500/v1/catalog/register

curl -X PUT -d '{"Datacenter": "cd", "Node": "CDM1E05-209010161", "Address": "10.209.10.161","Service": {"Service": "ngerr_shipper", "tags": ["monitor","nginx"] }}' http://127.0.0.1:8500/v1/catalog/register

# service discovery
dig @127.0.0.1 -p 8600 kafka.service.consul SRV # [TAG].NAME.service.consul
dig @localhost -p 8600 CDM1C03-209018065.node.consul

curl http://localhost:8500/v1/catalog/services
curl http://localhost:8500/v1/catalog/service/kafka


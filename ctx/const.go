package ctx

const (
	DefaultConfig = `
{
    zones: [
        {
            "name": "local"
            "zk": "localhost:2181"
            "influxdb": "localhost:8086"
            "swf": "http://localhost:9195/v1"
        }
        
    ]

    zk_default_zone: "local"
    kafka_home: "/opt/kafka_2.10-0.8.2.2"
    upgrade_center: "http://127.0.0.1"

    aliases: [
        {
            "cmd": "toplocal"
            "alias": "top -z local"
        }
        
    ]

    reverse_dns: [
        
    ]    
}
`
)

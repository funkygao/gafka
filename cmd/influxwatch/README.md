# influxwatch

A daemon that continuously watch influxdb access log to analyze the latency and payload size trend.

### How to run

tail -F /var/log/influxdb/influxd.log | influxwatch -z prod

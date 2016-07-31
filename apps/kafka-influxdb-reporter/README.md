Kafka InfluxDB Metrics Reporter
==============================


Install On Broker
------------

1. Build the `kafka-influxdb-reporter-1.0.0-uber.jar` jar using `mvn package`.
2. Add `kafka-influxdb-reporter-1.0.0-uber.jar` to the `libs/` 
   directory of your kafka broker installation
3. Configure the broker (see the configuration section below)
4. Restart the broker

Configuration
------------

Edit the `server.properties` file of your installation, activate the reporter by setting:

    kafka.metrics.polling.interval.secs=10
    kafka.metrics.reporters=eco.windrain.kafka.KafkaInfluxdbMetricsReporter

if you want multi reporters, just append others like this:
	
	kafka.metrics.reporters=eco.windrain.kafka.KafkaInfluxdbMetricsReporter,kafka.metrics.KafkaCSVMetricsReporter

Here is a list of default properties used:

    kafka.influxdb.metrics.reporter.enabled=true
	kafka.influxdb.metrics.host=localhost
	kafka.influxdb.metrics.port=8086
	kafka.influxdb.metrics.database=kafka
	kafka.influxdb.metrics.prefix=kafka
	kafka.influxdb.metrics.thread=influx-report
	# This can be use to exclude some metrics from influxdb
	# since kafka has quite a lot of metrics, it is useful
	# if you have many topics/partitions.
	kafka.influxdb.metrics.exclude.regex=<not set>

CSV reporter properties:

	kafka.csv.metrics.reporter.enabled=true
    kafka.csv.metrics.dir=/tmp/kafka_metrics


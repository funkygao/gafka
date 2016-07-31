package eco.windrain.kafka;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

public class KafkaInfluxdbMetricsReporter implements KafkaMetricsReporter,
	KafkaInfluxdbMetricsReporterMBean {

	final static Logger LOG = Logger.getLogger(KafkaInfluxdbMetricsReporter.class);
	
	volatile boolean initialized = false;
	volatile boolean running = false;
	
	private InfluxdbReporter reporter = null;

	private KafkaConfig kafkaConfig;

	@Override
	public String getMBeanName() {
		return "kafka:type=" + KafkaInfluxdbMetricsReporter.class.getName();
	}

	@Override
	public synchronized void startReporter(long pollingPeriodSecs) {
		if (initialized && !running) {
			reporter.start(pollingPeriodSecs, TimeUnit.SECONDS); // start executor
			running = true;
			LOG.info(String.format("Started Kafka Influxdb metrics reporter with polling period %d seconds", pollingPeriodSecs));
		}
	}

	@Override
	public synchronized void stopReporter() {
		if (initialized && running) {
			reporter.shutdown();
			running = false;
			LOG.info("Stopped Kafka Influxdb metrics reporter");
            try {
            	reporter = new InfluxdbReporter(kafkaConfig);
            } catch (IOException e) {
            	LOG.error("Unable to initialize InfluxdbReporter", e);
            }
		}
	}

	@Override
	public synchronized void init(VerifiableProperties props) {
		if (!initialized) {
			KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
			kafkaConfig = KafkaConfig.parseKafkaConfig(props);
			
			LOG.info("Initialize InfluxdbReporter: " + kafkaConfig.getInfluxConfig() + "; broker: " + kafkaConfig.getBrokerConfig());

            try {
            	/*
            	 * the reporter's job is to iterate each item in Metrics.defaultRegistry() and send its snapshot to influxdb.
            	 * all this is done in another thread poll tagged by 'influxdb-reporter' (usually 'metrics-influxdb-reporter-thread-1') 
            	 * so don't worry about writing blocked on kafka.
            	 * */
            	reporter = new InfluxdbReporter(kafkaConfig); // an executor

            } catch (IOException e) {
            	LOG.error("Unable to initialize InfluxdbReporter", e);
            }
            if (kafkaConfig.getInfluxConfig().isEnnable()) { // default disable
            	initialized = true;
            	startReporter(metricsConfig.pollingIntervalSecs()); //default value: kafka.metrics.polling.interval.secs=10
                LOG.debug("InfluxdbReporter started.");
            }
        }
    }
}

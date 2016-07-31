package eco.windrain.kafka;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

public class InfluxdbReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
	
	private static final Logger LOG = Logger.getLogger(InfluxdbReporter.class);
	
	protected TimeSeriesListener timeSeriesListener;
	
	protected KafkaConfig kafkaConfig;
	
	protected final Clock clock;
	
	protected final VirtualMachineMetrics vmMetrics;

	protected final Locale locale = Locale.US;


	/**
	 * Creates a new {@link InfluxdbReporter}.
	 * 
	 * @param metricsRegistry
	 *            the metrics registry
	 * @param clock
	 *            a {@link Clock} instance
	 * @param kafkaConfig
	 *            kafka config info
	 * @throws IOException
	 *        	if there is an error connecting to the Influxdb server
	 */
	public InfluxdbReporter(MetricsRegistry metricsRegistry, Clock clock, KafkaConfig kafkaConfig) throws IOException {
		super(metricsRegistry, kafkaConfig.getInfluxConfig().getThreadName());
		this.clock = clock;
		this.vmMetrics = VirtualMachineMetrics.getInstance();
		this.kafkaConfig = kafkaConfig;
	}
	
	
	public InfluxdbReporter(KafkaConfig kafkaConfig) throws IOException {
		this(Metrics.defaultRegistry(), Clock.defaultClock(), kafkaConfig);
		this.timeSeriesListener = new TimeSeriesInfluxdb(); // default handler
	}

	private AtomicLong runCalled = new AtomicLong();
	
	/**
     * The method called when a a poll is scheduled to occur.
     * executor.scheduleWithFixedDelay(this, period, period, unit);
     */
	@Override
	public void run() { // callback periodically
		try {
			final long epoch = clock.time() / 1000;
			
			if (kafkaConfig.getInfluxConfig().isEnnable()) {
				traveJvmMetrics(epoch);
				traveKafkaMetrics(epoch);
			} else {
				LOG.debug("influx report disable");
			}
			
		} catch (Exception e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Error writing to Influxdb", e);
			} else {
				LOG.warn("Error writing to Influxdb: " + e.getMessage());
			}
		} finally {
			LOG.debug("influxdb reporter runCalled " + runCalled.incrementAndGet());
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();
		// close other resources
		LOG.info("shutdown");
	}

	/** Metric Handler */
	protected void handleEvent(String name, String value, TimeSeriesType statType) throws IOException {
		//  Pub-Sub
		try {
			timeSeriesListener.onTimeSeries(name, value, statType, new TimeSeriesContext(clock.time() / 1000, kafkaConfig) );
		} catch (Throwable t) {
			LOG.error("time series listener exception", t);
		}
	}

	protected void traveKafkaMetrics(final Long epoch) {
		/* iterate each item in default metric container for kafka broker */
		MetricPredicate predicate = kafkaConfig.getInfluxConfig().getIncludePredicate();
		for (Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
			for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
				final MetricName metricName = subEntry.getKey();
				final Metric metric = subEntry.getValue();
				if (metric != null) {
					try {
						metric.processWith(this, metricName, epoch); // callback method stays in this object
					} catch (Exception ignored) {
						LOG.error("Error printing regular metrics:", ignored);
					}
				}
			}
		}
	}
	
	protected void traveJvmMetrics(long epoch) throws IOException {
		handleFloat("kafka.jvm.memory", "heap_usage", vmMetrics.heapUsage());
		handleFloat("kafka.jvm.memory", "non_heap_usage", vmMetrics.nonHeapUsage());
		for (Entry<String, Double> pool : vmMetrics.memoryPoolUsage().entrySet()) {
			handleFloat("kafka.jvm.memory.memory_pool_usages", sanitizeString(pool.getKey()), pool.getValue());
		}

		handleInt("kafka.jvm", "daemon_thread_count", vmMetrics.daemonThreadCount());
		handleInt("kafka.jvm", "thread_count", vmMetrics.threadCount());
		handleInt("kafka.jvm", "uptime", vmMetrics.uptime());
		handleFloat("kafka.jvm", "fd_usage", vmMetrics.fileDescriptorUsage());

		for (Entry<State, Double> entry : vmMetrics.threadStatePercentages().entrySet()) {
			handleFloat("kafka.jvm.thread-states", entry.getKey().toString().toLowerCase(), entry.getValue());
		}

		for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vmMetrics.garbageCollectors().entrySet()) {
			final String name = "kafka.jvm.gc." + sanitizeString(entry.getKey());
			handleInt(name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
			handleInt(name, "runs", entry.getValue().getRuns());
		}
	}

	protected void handleInt(String parentName, String subName, long value) throws IOException {
		handleEvent(parentName + "." + subName, format(value), TimeSeriesType.GAUGE);
	}

	protected void handleFloat(String name, String valueName, double value) throws IOException {
		handleEvent(name + "." + valueName, format(value), TimeSeriesType.GAUGE);
	}
	
	protected String sanitizeName(MetricName name) {
		// No Scope MBean:  kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec 
		// => kafka.server.BrokerTopicMetrics.MessagesInPerSec 
		final StringBuilder sb = new StringBuilder()
				.append(name.getGroup()).append('.')
				.append(name.getType()).append('.')
				.append(name.getName());
		
		// Scoped MBean:    kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower}
		// => kafka.network.RequestMetrics.RequestsPerSec@request.Produce
		if (name.hasScope()) {
			sb.append("@").append(name.getScope());
		}
		
		/*
		 * Topic & Partition Related Data: (Format is not pretty)
		 * kafka.log.Log.LogEndOffset@partition.0.topic.test
		 * kafka.log.Log.LogStartOffset@partition.0.topic.test
		 * 
		 * 
		 * kafka.server.BrokerTopicMetrics.BytesInPerSec
		 * kafka.server.BrokerTopicMetrics.BytesInPerSec@topic.test.count
		 * */
		return sb.toString();
	}

	protected String sanitizeString(String s) {
		return s.replace(' ', '-');
	}
	
	@Override
	public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) throws IOException {
		handleEvent(sanitizeName(name), gauge.value().toString(), TimeSeriesType.GAUGE);
	}

	@Override
	public void processCounter(MetricName name, Counter counter, Long epoch) throws IOException {
		handleEvent(sanitizeName(name) + ".count", format(counter.count()), TimeSeriesType.COUNTER);
	}

	@Override
	public void processMeter(MetricName name, Metered meter, Long epoch) throws IOException {
		final String sanitizedName = sanitizeName(name);
		handleEvent(sanitizedName + ".count", format(meter.count()), TimeSeriesType.GAUGE);
		handleEvent(sanitizedName + ".m1_rate", format(meter.oneMinuteRate()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".m5_rate", format(meter.fiveMinuteRate()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".m15_rate", format(meter.fifteenMinuteRate()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".mean_rate", format(meter.meanRate()), TimeSeriesType.TIMER);
	}

	@Override
	public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
		final String sanitizedName = sanitizeName(name);
		handleSummarizable(sanitizedName, histogram);
		handleSampling(sanitizedName, histogram);
	}

	@Override
	public void processTimer(MetricName name, Timer timer, Long epoch) throws IOException {
		processMeter(name, timer, epoch);
		final String sanitizedName = sanitizeName(name);
		handleSummarizable(sanitizedName, timer);
		handleSampling(sanitizedName, timer);
	}

	protected void handleSummarizable(String sanitizedName, Summarizable metric) throws IOException {
		handleEvent(sanitizedName + ".count", format(metric.sum()), TimeSeriesType.GAUGE);
		handleEvent(sanitizedName + ".max", format(metric.max()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".mean", format(metric.mean()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".min", format(metric.min()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".stddev", format(metric.stdDev()), TimeSeriesType.TIMER);
	}

	protected void handleSampling(String sanitizedName, Sampling metric) throws IOException {
		final Snapshot snapshot = metric.getSnapshot();
		handleEvent(sanitizedName + ".p50", format(snapshot.getMedian()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".p75", format(snapshot.get75thPercentile()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".p95", format(snapshot.get95thPercentile()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".p98", format(snapshot.get98thPercentile()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".p99", format(snapshot.get99thPercentile()), TimeSeriesType.TIMER);
		handleEvent(sanitizedName + ".p999", format(snapshot.get999thPercentile()), TimeSeriesType.TIMER);
	}

	private String format(long n) {
		return String.format(locale, "%d", n);
	}

	private String format(double v) {
		return String.format(locale, "%2.2f", v);
	}


}

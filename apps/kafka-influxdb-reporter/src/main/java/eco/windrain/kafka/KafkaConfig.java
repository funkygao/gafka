package eco.windrain.kafka;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

import eco.windrain.kafka.util.HostUtil;
import kafka.utils.VerifiableProperties;

public class KafkaConfig {
	
	
	private BrokerConfig brokerConfig;
	
	private InfluxConfig influxConfig;

	private KafkaConfig(BrokerConfig brokerConfig, InfluxConfig influxConfig) {
		super();
		this.brokerConfig = brokerConfig;
		this.influxConfig = influxConfig;
	}

	public BrokerConfig getBrokerConfig() {
		return brokerConfig;
	}

	public InfluxConfig getInfluxConfig() {
		return influxConfig;
	}
	
	public static KafkaConfig parseKafkaConfig(VerifiableProperties props) {
		return new KafkaConfig(parseBrokerConfig(props), parseInfluxConfig(props));
	}
	

	public static BrokerConfig parseBrokerConfig(VerifiableProperties props) {
		return new BrokerConfig(
				props.getString("host.name", HostUtil.getHostIp()), 
				props.getInt("port", 9092), 
				props.getInt("broker.id")
				);
	}
	
	public static InfluxConfig parseInfluxConfig(VerifiableProperties props) {
		return new InfluxConfig(
				props.getBoolean("kafka.influxdb.metrics.reporter.enabled", false), 
				props.getString("kafka.influxdb.metrics.host", "localhost").trim(), 
				props.getInt("kafka.influxdb.metrics.port", 8086), 
				props.getString("kafka.influxdb.metrics.database", "kafka"), 
				props.getString("kafka.influxdb.metrics.prefix", "").trim(), 
				props.getString("kafka.influxdb.metrics.exclude.regex", null),
				props.getString("kafka.influxdb.metrics.thread", "influx-report")
				);
	}
	

	public static class BrokerConfig {
		
		private String brokerHost;
		
		private Integer brokerPort;
		
		private Integer brokerId;
		
		private BrokerConfig(String brokerHost, Integer brokerPort, Integer brokerId) {
			super();
			this.brokerHost = brokerHost;
			this.brokerPort = brokerPort;
			this.brokerId = brokerId;
		}

		public String getBrokerHost() {
			return brokerHost;
		}

		public Integer getBrokerPort() {
			return brokerPort;
		}

		public Integer getBrokerId() {
			return brokerId;
		}

		@Override
		public String toString() {
			return "BrokerConfig [brokerHost=" + brokerHost + ", brokerPort=" + brokerPort + ", brokerId=" + brokerId
					+ "]";
		}

	}
	
	
	public static class InfluxConfig {
		
		private String influxHost;
		
		private Integer influxPort;
		
		private String influxDatabase;
		
		private boolean ennable;
		
		private String groupPrefix;
		
		private String excludeRegex;
		
		private MetricPredicate includePredicate;
		
		private String threadName;

		private InfluxConfig(boolean ennable, String influxHost, Integer influxPort, String database, String groupPrefix,
				String excludeRegex, String threadName) {
			super();
			this.influxHost = influxHost;
			this.influxPort = influxPort;
			this.influxDatabase = database;
			this.ennable = ennable;
			
			if (groupPrefix != null && groupPrefix.length() != 0) {
				// Pre-append the "." so that we don't need to make anything
				// conditional later.
				this.groupPrefix = groupPrefix + ".";
			} else {
				this.groupPrefix = "";
			}
			
			this.excludeRegex = excludeRegex;
			if (excludeRegex == null) {
				includePredicate = MetricPredicate.ALL;
			} else {
				includePredicate = new RegexMetricPredicate(excludeRegex);
			}
			
			this.threadName = threadName;
		}

		@Override
		public String toString() {
			return "InfluxConfig [influxHost=" + influxHost + ", influxPort=" + influxPort + ", influxDatabase="
					+ influxDatabase + ", ennable=" + ennable + ", groupPrefix=" + groupPrefix + ", excludeRegex="
					+ excludeRegex + ", includePredicate=" + includePredicate + ", threadName=" + threadName + "]";
		}


		public String getInfluxHost() {
			return influxHost;
		}

		public Integer getInfluxPort() {
			return influxPort;
		}
		

		public String getInfluxDatabase() {
			return influxDatabase;
		}

		public boolean isEnnable() {
			return ennable;
		}

		public String getGroupPrefix() {
			return groupPrefix;
		}

		public String getExcludeRegex() {
			return excludeRegex;
		}

		public MetricPredicate getIncludePredicate() {
			return includePredicate;
		}

		public String getThreadName() {
			return threadName;
		}
		
	}
	
	public static class RegexMetricPredicate implements MetricPredicate {

		Pattern pattern = null;
		static Logger LOG = Logger.getLogger(RegexMetricPredicate.class);
		
		public RegexMetricPredicate(String regex) {
			pattern = Pattern.compile(regex);
		}
		
		@Override
		public boolean matches(MetricName name, Metric metric) {
			boolean ok = !pattern.matcher(name.getName()).matches();
			LOG.debug(String.format("name: %s - %s", name.getName(), ok));
			return ok;
		}
	}
}

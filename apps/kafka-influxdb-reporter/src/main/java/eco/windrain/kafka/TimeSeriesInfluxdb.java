package eco.windrain.kafka;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

public class TimeSeriesInfluxdb implements TimeSeriesListener {
	
	private static final Logger LOG = Logger.getLogger(TimeSeriesInfluxdb.class);

	private InfluxDB influxDB;
	
	@Override
	public void onTimeSeries(String key, String value, TimeSeriesType statType, TimeSeriesContext context) {
		if (key.equals("kafka.common.AppInfo.Version")) {
			return;
		}
		
		String measurement = context.getKafkaConfig().getInfluxConfig().getGroupPrefix() + key;
		String influxHost = context.getKafkaConfig().getInfluxConfig().getInfluxHost();
		int influxPort = context.getKafkaConfig().getInfluxConfig().getInfluxPort();
		String influxDatabase = context.getKafkaConfig().getInfluxConfig().getInfluxDatabase();
		long epochTime = context.getEpochSecond();
		
		String brokerHost = context.getKafkaConfig().getBrokerConfig().getBrokerHost();
		Integer brokerPort = context.getKafkaConfig().getBrokerConfig().getBrokerPort();
		Integer brokerId = context.getKafkaConfig().getBrokerConfig().getBrokerId();

		LOG.debug("TS >> " +  measurement + "=" + value);
		
		if (influxDB == null) {
			LOG.info("connecting influxdb: " + "http://"+influxHost+":"+influxPort);
			try {
				influxDB = InfluxDBFactory.connect("http://"+influxHost+":"+influxPort, "root", "root");
				LOG.info("connected influxdb: " + "http://"+influxHost+":"+influxPort);
			} catch (Throwable e) {
				LOG.warn("connect influxdb error" + "http://"+influxHost+":"+influxPort,  e);
			}
		}
		
		if (value == null || value.length() == 0) {
			return ;
		}

		Object valueTyped;
		try {
			valueTyped = (value.indexOf(".") != -1 ? Double.parseDouble(value.trim()) : Long.parseLong(value.trim()));
		} catch (NumberFormatException e) {
			LOG.warn(key+"="+value);
			return;
		}
		
		BatchPoints batchPoints = BatchPoints
                .database(influxDatabase)
                .retentionPolicy("default")
                .consistency(ConsistencyLevel.ALL)
                .build();
		
		Point point1 = Point.measurement(measurement)
                    .time(epochTime, TimeUnit.SECONDS)
                    .field("value", valueTyped) // digital type required when using mean/mode/max/min aggregation functions in fluxdb query
                    .tag("bhost", brokerHost)
                    .tag("bport", brokerPort.toString())
                    .tag("broker", brokerId.toString())
                    .tag("type", statType.toString())
                    .build();
		batchPoints.point(point1);
		
		try {
			influxDB.write(batchPoints); // 提交批量写，包含两个写入x
			
		} catch (Exception ew) {
			LOG.error("write influxdb error", ew);
		}
	}

}

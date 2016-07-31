package eco.windrain.kafka;

public class TimeSeriesContext {

	private long epochSecond;
	
	private KafkaConfig kafkaConfig;

	public TimeSeriesContext(long epochSecond, KafkaConfig kafkaConfig) {
		super();
		this.epochSecond = epochSecond;
		this.kafkaConfig = kafkaConfig;
	}

	public long getEpochSecond() {
		return epochSecond;
	}

	public KafkaConfig getKafkaConfig() {
		return kafkaConfig;
	}
	
}

package eco.windrain.kafka;

public interface TimeSeriesListener {

	public void onTimeSeries(String key, String value, TimeSeriesType statType, TimeSeriesContext context);
	
}

package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Sample struct {
	Ui  cli.Ui
	Cmd string

	mode string
}

func (this *Sample) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("consumers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.mode, "m", "c", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	var code string
	switch this.mode {
	case "c":
		code = this.consumeSample()

	case "p":
		code = this.produceSample()
	}
	this.Ui.Output(strings.TrimSpace(code))

	return
}

func (*Sample) produceSample() string {
	return fmt.Sprintf(`
public class KafkaProducer {
    private final Producer<String, String> producer;
    public final static String TOPIC = "TEST-TOPIC";
 
    private KafkaProducer(){
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.193.148:9092,192.168.193.149:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
 
        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");
 
        producer = new Producer<String, String>(new ProducerConfig(props));
    }
 
    void produce() {
        int messageNo = 1000;
        final int COUNT = 10000;
 
        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key ,data));
            messageNo ++;
        }
    }
 
    public static void main(String[] args) {
        new KafkaProducer().produce();
    }
}
		`)
}

func (*Sample) consumeSample() string {
	return fmt.Sprintf(`
public class KafkaConsumer {

    private final ConsumerConnector consumer;

    private KafkaConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "12.1.12.12:2181,12.1.12.13:2181:12.1.12.13:2181/kafka");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000"); // 1m
        props.put("auto.offset.reset", "smallest");    // largest|smallest
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
   }

    void consume(String topic, int threads) {
    	// VERY important!
        Runtime.getRuntime().addShutdownHook(new Thread() {
        	public void run() {
        		example.shutdown();
        	}
        });

    	Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threads);

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println(it.next().message());
        }
    }

    public static void main(String[] args) {
        new KafkaConsumer().consume();
    }
}	
		`)
}

func (*Sample) Synopsis() string {
	return "Sample code of kafka producer/consumer in Java"
}

func (this *Sample) Help() string {
	help := fmt.Sprintf(`
Usage: %s sample -m mode

    Sample code of kafka producer/consumer in Java

Options:

    -m <c|p>
      c=consume, p=produce

`, this.Cmd)
	return strings.TrimSpace(help)
}

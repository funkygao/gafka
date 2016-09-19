package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Sample struct {
	Ui  cli.Ui
	Cmd string

	mode string
}

func (this *Sample) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("sample", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.mode, "mode", "c", "")
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
        props.put("%s", "k10101a.mycorp.kfk.com:10101,k10101b.mycorp.kfk.com:10101");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
 
        // %s
        // 0, means that the producer never waits for an acknowledgement from the broker
        // 1, means that the producer gets an acknowledgement after the leader replica has received the data
        //-1, means that the producer gets an acknowledgement after all in-sync replicas have received the data
        props.put("%s", "1"); // 1 is enough in most cases
 
        producer = new Producer<String, String>(new ProducerConfig(props));
    }
 
    void produce() {
        int messageNo = 1000;
        final int COUNT = 10000;
 
        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
            messageNo ++;
        }
    }
 
    public static void main(String[] args) {
        new KafkaProducer().produce();
    }
}
        `,
		color.Cyan("metadata.broker.list"),
		color.Cyan("request.required.acks"),
		color.Cyan("request.required.acks"))
}

func (*Sample) consumeSample() string {
	return fmt.Sprintf(`
public class KafkaConsumer {
    private final ConsumerConnector consumer;

    private KafkaConsumer() {
        Properties props = new Properties();
        props.put("%s", "zk2181a.mycorp.zk.com:2181,zk2181b.mycorp.zk.com:2181,zk2181c.mycorp.zk.com:2181/kafka");
        props.put("%s", "group1");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000");   // 1m
        //props.put("auto.offset.reset", "smallest");    // largest | smallest
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }
   }

    void consume(String topic, int %s) {
        // %s
        // %s
        // %s
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                consumer.shutdown();
            }
        });

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, %s);

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()) {
            // consumer.commitOffsets(); // manually commit offsets
            System.out.println(it.next().message());
        }
    }

    public static void main(String[] args) {
        new KafkaConsumer().consume();
    }
}   
        `,
		color.Cyan("zookeeper.connect"),
		color.Cyan("group.id"),
		color.Green("threads"),
		color.Red("VERY important!"),
		color.Red("graceful shutdown the consumer group to commit consumed offset"),
		color.Red("avoid consuming duplicated message when restarting the same consumer group"),
		color.Green("threads"))
}

func (*Sample) Synopsis() string {
	return "Java sample code of producer/consumer"
}

func (this *Sample) Help() string {
	help := fmt.Sprintf(`
Usage: %s sample -mode mode

    %s

Options:

    -mode <c|p>
      c: consumer 
      p: producer

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}

package com.foo;

import com.wanda.pubsub.client.bean.*;
import com.wanda.pubsub.client.enums.StatusCodeEnum;
import com.wanda.pubsub.client.exception.AuthorityErrorException;
import com.wanda.pubsub.client.pub.PubClient;
import com.wanda.pubsub.client.sub.SubClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * PubSub demo!
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) {
        logger.info("Welcome to PubSub!");

        // test default kafka ProducerConfig
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:11");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);
        logger.info("{}", producer);

        // MODIFY the params before usage!!!
        boolean subMode = true;
        PubConfig pubConfig = new PubConfig("http://localhost:9191/v1/msgs/foobar/v1", "app1", "pubkey");
        SubConfig subConfig = new SubConfig("http://localhost:9192/v1/msgs/app1/foobar/v1?group=group1", "app2", "subkey");
        HttpConfig httpConfig = new HttpConfig();

        if (subMode) {
            demoSub(subConfig, httpConfig);
        } else {
            demoPub(pubConfig, httpConfig);
        }

        logger.info("Bye!");
    }

    public static void demoSub(SubConfig subConfig, HttpConfig httpConfig) {
        SubClient sbClient = new SubClient(subConfig, httpConfig);
        String offset = null;
        String patition = null;

        while (true) {
            try {
                SubResult result = sbClient.pullAsack(offset, patition);
                int statusCode = result.getStatusCode();
                if (statusCode != StatusCodeEnum.RECEIVE_SUCCESS.getStatus()) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                logger.info("status:{} message:<{}> content:<{}>", statusCode, result.getData(), result.getMessage());
            } catch (AuthorityErrorException ae) {
                ae.printStackTrace();
                break; // stop the world
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void demoPub(PubConfig pubConfig, HttpConfig httpConfig) {
        try {
            PubClient client = new PubClient(pubConfig, httpConfig, false);
            PubResult pubResult = client.send("key-001", "hello world");
            System.out.println(pubResult.getStatusCode() + ":partition=" + pubResult.getPartition() + ":offset=" + pubResult.getOffset());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

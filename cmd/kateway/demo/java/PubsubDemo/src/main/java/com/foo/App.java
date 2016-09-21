package com.foo;

import com.wanda.pubsub.client.bean.*;
import com.wanda.pubsub.client.enums.StatusCodeEnum;
import com.wanda.pubsub.client.exception.AuthorityErrorException;
import com.wanda.pubsub.client.pub.PubClient;
import com.wanda.pubsub.client.sub.SubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PubSub demo!
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);


    public static void main(String[] args) {
        logger.info("Welcome to PubSub!");

        // MODIFY the params before usage!!!
        boolean subMode = true;
        PubConfig pubConfig = new PubConfig("http://pub.foo.com/v1/msgs/foobar/v1", "app1", "pubkey");
        SubConfig subConfig = new SubConfig("http://sub.foo.com/v1/msgs/app1/foobar/v1?group=mygroup1", "app2", "subkey");
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

                logger.trace("status:{} content:{}", statusCode, result.getData());
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

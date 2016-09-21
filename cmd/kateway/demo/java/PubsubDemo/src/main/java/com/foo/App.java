package com.foo;

import com.wanda.pubsub.client.bean.HttpConfig;
import com.wanda.pubsub.client.bean.PubConfig;
import com.wanda.pubsub.client.bean.SubConfig;
import com.wanda.pubsub.client.bean.SubResult;
import com.wanda.pubsub.client.enums.StatusCodeEnum;
import com.wanda.pubsub.client.exception.AuthorityErrorException;
import com.wanda.pubsub.client.sub.SubClient;

/**
 * PubSub demo!
 */
public class App {

    public static void main(String[] args) {
        System.out.println("Welcome to PubSub!");

        PubConfig pubConfig;
        SubConfig subConfig = new SubConfig("http://sub.foo.com/", "app1", "subkey");
        HttpConfig httpConfig = new HttpConfig();

        demoSub(subConfig, httpConfig);
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
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                System.out.println(result);

            } catch (AuthorityErrorException ae) { //需要终止线程
                ae.printStackTrace();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void demoPub() {

    }
}

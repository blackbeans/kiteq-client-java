package org.kiteq.standalone;

import org.kiteq.client.ClientConfigs;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KiteConsumer {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KiteConsumer.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "s-mts-test";
    private static final String SECRET_KEY = "123456";
    
    private KiteClient consumer;
    
    public KiteConsumer() {
        consumer = new DefaultKiteClient(ZK_ADDR, new ClientConfigs(GROUP_ID, SECRET_KEY), new ListenerAdapter() {
            @Override
            public boolean onMessage(Message message) {
//                logger.warn("recv: {}", message);
                return true;
            }
        });
        consumer.setBindings(new Binding[]{
                Binding.bindDirect(GROUP_ID, "trade", "pay-succ", 1000, true),
                Binding.bindDirect(GROUP_ID, "trade", "hello", 1000, true),
                Binding.bindDirect(GROUP_ID, "feed", "add", 1000, true),
        });
    }
    
    public void start() {
        consumer.start();
//        KiteStats.close();
    }
    
    public static void main(String[] args) {
        Thread.currentThread().setName("KiteqConsumer");
        System.setProperty("kiteq.appName", "Consumer");
        new KiteConsumer().start();
    }
}

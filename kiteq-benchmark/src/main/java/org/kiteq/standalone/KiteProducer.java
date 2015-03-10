package org.kiteq.standalone;

import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.commons.util.JsonUtils;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class KiteProducer {
    private static final Logger logger = LoggerFactory.getLogger(KiteProducer.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    private static final String TOPOIC = "trade";
    
    private KiteClient producer;
    
    public KiteProducer() {
        producer = new DefaultKiteClient(ZK_ADDR, new ClientConfigs(GROUP_ID, SECRET_KEY), new ListenerAdapter() {
            @Override
            public void onMessageCheck(TxResponse response) {
                logger.warn(JsonUtils.prettyPrint(response));
                response.commit();
            }
        });
        producer.setPublishTopics(new String[] { TOPOIC });
    }
    
    private StringMessage buildMessage() {
        String messageId = UUID.randomUUID().toString();
        Header header = Header.newBuilder()
                .setMessageId(messageId)
                .setTopic(TOPOIC)
                .setMessageType("pay-succ")
                .setExpiredTime(System.currentTimeMillis())
                .setDeliverLimit(-1)
                .setGroupId("go-kite-test")
                .setCommit(false)
                .setFly(true).build();
        
        StringMessage message = StringMessage.newBuilder()
                .setHeader(header)
                .setBody("echo").build();
        return message;
    }
    
    public void start() {
        producer.start();
        while (!Thread.currentThread().isInterrupted()) {
            SendResult result = producer.sendStringMessage(buildMessage());
//            logger.warn("Send result: {}", result);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        producer.close();
    }
    
    public static void main(String[] args) {
        Thread.currentThread().setName("KiteqProducer");
        System.setProperty("kiteq.appName", "Producer");
        for (int i = 0; i < 1; ++i) {
            new KiteProducer().start();
        }
    }
}

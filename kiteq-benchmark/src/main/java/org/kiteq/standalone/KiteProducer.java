package org.kiteq.standalone;

import java.util.UUID;

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

public class KiteProducer {
    private static final Logger logger = LoggerFactory.getLogger(KiteProducer.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    private static final String TOPOIC = "trade";
    
    private KiteClient producer;
    
    public KiteProducer() {
        producer = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY, new ListenerAdapter() {
            @Override
            public void onMessageCheck(TxResponse response) {
                logger.warn(JsonUtils.prettyPrint(response));
                response.commint();
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
                .setCommit(false).build();
        
        StringMessage message = StringMessage.newBuilder()
                .setHeader(header)
                .setBody("echo").build();
        return message;
    }
    
    public void start() {
        producer.start();
        
        SendResult result = producer.sendStringMessage(buildMessage());
        logger.warn("Send result: {}", result);
        
        producer.close();
    }
    
    public static void main(String[] args) {
        System.setProperty("kiteq.appName", "Producer");
        new KiteProducer().start();
    }
}

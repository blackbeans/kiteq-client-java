package org.kiteq.standalone;

import java.util.UUID;

import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class StandaloneKitePublisher {
    
    private static final Logger logger = LoggerFactory.getLogger(StandaloneKitePublisher.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    
    private KiteClient publisher;
    
    public StandaloneKitePublisher() {
        publisher = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY, new MessageListener() {
            
            @Override
            public boolean onMessage(StringMessage message) {
                logger.warn("recv: {}", message.toString());
                return true;
            }

            @Override
            public void onMessageCheck(String messageId, TxResponse response) {
            }
        });
    }
    
    private StringMessage buildMessage() {
        
        String messageId = UUID.randomUUID().toString();
        
        Header header = Header.newBuilder()
                .setMessageId(messageId)
                .setTopic("trade")
                .setMessageType("pay-succ")
                .setExpiredTime(System.currentTimeMillis())
                .setDeliverLimit(-1)
                .setGroupId("go-kite-test")
                .setCommit(true).build();
        
        StringMessage message = StringMessage.newBuilder()
                .setHeader(header)
                .setBody("echo").build();
        
        return message;
    }
    
    public void start() {
        publisher.start();
        
        StringMessage message = buildMessage();
        
        for (int i = 0; i < 10; i++) {
            SendResult result = publisher.sendStringMessage(message);
            logger.warn("{}, {}", i, result.toString());
        }
        
        publisher.close();
    }
    
    public static void main(String[] args) {
        new StandaloneKitePublisher().start();
    }

}

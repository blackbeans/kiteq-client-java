package org.kiteq.standalone;

import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.MessageStatus;
import org.kiteq.client.message.SendResult;
import org.kiteq.commons.message.Message;
import org.kiteq.commons.message.StringMessage;
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
            public void receiveMessage(Message message, MessageStatus status) {
                logger.warn("recv: {}", message.toString());
            }
        });
    }
    
    private Message buildMessage() {
        
        long currentTime = System.currentTimeMillis();
        
        StringMessage message = new StringMessage();
        message.setMessageId("" + currentTime);
        message.setTopic("trade");
        message.setMessageType("pay-succ");
        message.setExpiredTime(currentTime);
        message.setDeliveryLimit(-1);
        message.setGroupId("go-kite-test");
        message.setCommit(true);
        message.setBody("echo");
        
        return message;
    }
    
    public void start() {
        publisher.start();
        
        Message message = buildMessage();
        
        for (int i = 0; i < 10; i++) {
            SendResult result = publisher.sendMessage(message);
            logger.warn("{}, {}", i, result.toString());
        }
        
        publisher.close();
    }
    
    public static void main(String[] args) {
        new StandaloneKitePublisher().start();
    }

}

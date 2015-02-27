package org.kiteq.standalone;

import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.MessageStatus;
import org.kiteq.commons.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class StandaloneKiteSubscriber {
    
    private static final Logger logger = LoggerFactory.getLogger(StandaloneKiteSubscriber.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "s-mts-test";
    private static final String SECRET_KEY = "123456";
    
    private KiteClient subscriber;
    
    public StandaloneKiteSubscriber() {
        
        subscriber = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY, new MessageListener() {
            
            @Override
            public void receiveMessage(Message message, MessageStatus status) {
                if (logger.isDebugEnabled()) {
                    logger.debug("recv: {}", message.toString());
                }
            }
        });
    }
    
    public void start() {
        subscriber.start();
    }
    
    public static void main(String[] args) {
        System.setProperty("kiteq.appName", "Consumer");
        new StandaloneKiteSubscriber().start();
    }

}

package org.kiteq.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.MessageStatus;
import org.kiteq.commons.message.Message;
import org.kiteq.commons.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class KiteSubscriberTest {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteSubscriberTest.class);
    
    private static final String GROUP_ID = "s-mts-test";
    
    private KiteSubscriber subscriber;
    
    @Before
    public void init() {
        subscriber = new DefaultKiteClient(GROUP_ID);
    }
    
    @After
    public void close() {
        ThreadUtils.sleep(1000 * 30);
    }
    
    @Test
    public void testSubscribe() throws Exception {
        subscriber.setMessageListener(new MessageListener() {
            
            @Override
            public void receiveMessage(Message message, MessageStatus status) {
                logger.warn("recv: {}", message.toString());
            }
        });
    }

}

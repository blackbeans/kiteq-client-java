package org.kiteq.client;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.SendResult;
import org.kiteq.commons.message.Message;
import org.kiteq.commons.message.StringMessage;
import org.kiteq.commons.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class KitePublisherTest {
    
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KitePublisherTest.class);
    
    private static final String GROUP_ID = "s-mts-test";
    
    private KitePublisher publisher;
    
    @Before
    public void init() {
        publisher = new DefaultKiteClient(GROUP_ID);
    }
    
    @After
    public void close() {
        ThreadUtils.sleep(3000);
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
    
    @Test
    public void testSendMessage() throws Exception {
        
        Message message = buildMessage();
        SendResult result = publisher.sendMessage(message);
        
        Assert.assertEquals(true, result.isSuccess());
    }

}

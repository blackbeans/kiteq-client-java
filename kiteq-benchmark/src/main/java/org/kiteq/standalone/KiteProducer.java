package org.kiteq.standalone;

import java.util.UUID;

import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.commons.stats.MessageStats;
import org.kiteq.commons.util.JsonUtils;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class KiteProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteProducer.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    
    private KiteClient producer;
    
    public KiteProducer() {
        producer = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY, new MessageListener() {
            
            @Override
            public boolean onStringMessage(StringMessage message) {
                // TODO Auto-generated method stub
                return false;
            }
            
            @Override
            public void onMessageCheck(TxResponse response) {
                logger.warn(JsonUtils.prettyPrint(response));
                response.setStatus(1);
            }
            
            @Override
            public boolean onBytesMessage(BytesMessage message) {
                // TODO Auto-generated method stub
                return false;
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
                .setCommit(false).build();
        
        StringMessage message = StringMessage.newBuilder()
                .setHeader(header)
                .setBody("echo").build();
        
        return message;
    }
    
    public void start() {
        producer.start();
        
        StringMessage message = buildMessage();
        
        for (int i = 0; i < 1; i++) {
            SendResult result = producer.sendStringMessage(message);
            logger.warn("{}, {}", i, result.toString());
        }
        
        producer.close();
        MessageStats.close();
    }
    
    public static void main(String[] args) {
        new KiteProducer().start();
    }

}

package org.kiteq.standalone;

import com.google.protobuf.ByteString;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.commons.util.JsonUtils;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
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

    private KiteRemoting.BytesMessage buildMessage() {
        String messageId = UUID.randomUUID().toString();
        Header header = Header.newBuilder()
                .setMessageId(messageId)
                .setTopic(TOPOIC)
                .setMessageType("pay-succ")
                .setExpiredTime(System.currentTimeMillis())
                .setDeliverLimit(-1)
                .setGroupId("go-kite-test")
                .setCommit(true)
                .setFly(true).build();
        byte[] bytes = new byte[512];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 512; ++i) {
            int anInt = random.nextInt(127);
            if (anInt == 10 || anInt == 13) {
                anInt += 1;
            }
            bytes[i] = (byte) anInt;
        }
        return KiteRemoting.BytesMessage.newBuilder().setHeader(header).setBody(ByteString.copyFrom(bytes)).build();
    }
    
    public void start() {
        producer.start();
        while (!Thread.currentThread().isInterrupted()) {
            SendResult result = producer.sendBytesMessage(buildMessage());
            logger.warn("Send result: {}", result);

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

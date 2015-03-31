package org.kiteq.standalone;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomUtils;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.TxResponse;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KiteProducer {
    private static final Logger logger = LoggerFactory.getLogger(KiteProducer.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    private static final String TOPIC = "trade";
    
    private KiteClient producer;

    private static ConcurrentMap<String, KiteRemoting.BytesMessage> messages = new ConcurrentHashMap<String, KiteRemoting.BytesMessage>();
    
    public KiteProducer() {
        producer = new DefaultKiteClient(ZK_ADDR, new ClientConfigs(GROUP_ID, SECRET_KEY),
                new ListenerAdapter() {
                    @Override
                    public void onMessageCheck(TxResponse response) {
                        List<KiteRemoting.Entry> entries = response.getHeader().getPropertiesList();
                        for (KiteRemoting.Entry entry : entries) {
                            if (entry.getKey().equals("TxId")) {
                                String txId = entry.getValue();
                                KiteRemoting.BytesMessage remove = messages.remove(txId);
                                if (remove != null) {
                                    logger.info(remove.toString());
                                    response.commit();
                                } else {
                                    logger.warn("rollback " + txId);
                                    response.rollback();
                                }
                            }
                        }
                    }
        });
        producer.setPublishTopics(new String[]{TOPIC});
    }

    private KiteRemoting.BytesMessage buildMessage(boolean commit) {
        String messageId = UUID.randomUUID().toString();
        Header header = Header.newBuilder()
                .setMessageId(messageId)
                .setTopic(TOPIC)
                .setMessageType("pay-succ")
                .setExpiredTime(System.currentTimeMillis() / 1000 + TimeUnit.MINUTES.toSeconds(10))
                .setDeliverLimit(100)
                .setGroupId("go-kite-test")
                .setCommit(commit) // commit=false: 等待ack; true: 事务成功
                .setFly(true)
                .addProperties(KiteRemoting.Entry.newBuilder().setKey("TxId").setValue(messageId)).build();
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

    public void start() throws InterruptedException {
        producer.start();
        l:
        while (!Thread.currentThread().isInterrupted()) {
            long total = 1000;
            while (total-- > 0) {
                try {
                    boolean commit = RandomUtils.nextInt(0, 2) < 1;
                    KiteRemoting.BytesMessage message = buildMessage(commit);
                    if (commit) {
                        messages.put(message.getHeader().getMessageId(), message);
                        producer.sendTxMessage(message, new KiteClient.TxCallback() {
                            @Override
                            public void doTransaction(TxResponse txResponse) throws Exception {
                                String messageId = txResponse.getMessageId();
                                KiteRemoting.BytesMessage remove = messages.remove(messageId);
                                if (remove != null) {
                                    logger.info("handle transaction: " + remove.toString());
                                }
                            }
                        });
                    } else {
                        producer.sendBytesMessage(message);
                    }
                } catch (NoKiteqServerException e) {
                    Thread.currentThread().interrupt();
                    break l;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            TimeUnit.SECONDS.sleep(10);
        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.currentThread().setName("KiteqProducer");
        System.setProperty("kiteq.appName", "Producer");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("tx: " + messages.keySet());
            }
        }, 1, 1, TimeUnit.SECONDS);
        for (int i = 0; i < 1; ++i) {
            new KiteProducer().start();
        }
    }
}

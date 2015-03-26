package org.kiteq.benchmark;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.commons.util.JsonUtils;
import org.kiteq.commons.util.ParamUtils;
import org.kiteq.commons.util.ThreadUtils;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaofeihang
 * @since Jan 5, 2015
 */
public class KiteProducerBenchmark {

    static final AtomicLong messageId = new AtomicLong(0);
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(KiteProducerBenchmark.class);
    
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    private static final String TOPIC = "trade";

    private final int threadNum;

    private final int workerNum;

    private KiteClient[] clients;
    
    private ExecutorService executorService;

    private final long sendInterval;

    public KiteProducerBenchmark(String[] args) {
        Map<String, String> params = ParamUtils.parse(args);
        String zkAddr = StringUtils.defaultString(params.get("-zkAddr"), "localhost:2181");
        LOG.info("zkAddr=" + zkAddr);
        sendInterval = NumberUtils.toLong(params.get("-sendInterval"), 1000);
        LOG.info("sendInterval=" + sendInterval);
        threadNum = NumberUtils.toInt(params.get("-t"), Runtime.getRuntime().availableProcessors() * 2);
        LOG.info("threadNum=" + threadNum);
        workerNum = NumberUtils.toInt(params.get("-worker"), 10);
        LOG.info("workerNum=" + workerNum);

        clients = new KiteClient[threadNum];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultKiteClient(zkAddr, new ClientConfigs(GROUP_ID, SECRET_KEY));
            clients[i].setPublishTopics(new String[]{TOPIC});
            clients[i].start();
        }
        
        executorService = Executors.newCachedThreadPool();
    }
    
    public void start() {
        final CountDownLatch latch = new CountDownLatch(threadNum * workerNum);

        for (int i = 0; i < threadNum; i++) {
            final KiteClient client = clients[i];
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < workerNum; ++j) {
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                while (!Thread.currentThread().isInterrupted()) {
                                    try {
                                        client.sendBytesMessage(buildMessage());
                                    } catch (NoKiteqServerException e) {
                                        break;
                                    }

                                    if (sendInterval > 0) {
                                        ThreadUtils.sleep(sendInterval);
                                    }
                                }

                                latch.countDown();
                            }
                        }).start();
                    }
                }
            });
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        for (KiteClient client : clients) {
            client.close();
        }
        
        executorService.shutdown();
    }

    private static final AtomicLong ID = new AtomicLong(0);

    private KiteRemoting.BytesMessage buildMessage() {
        Header header = Header.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setTopic(TOPIC)
                .setMessageType("pay-succ")
                .setExpiredTime(System.currentTimeMillis() / 1000 + TimeUnit.MINUTES.toSeconds(10))
                .setDeliverLimit(100)
                .setGroupId("go-kite-test")
                .setCommit(true)
                .setFly(false).build();

        int payloadLength = 100;
        byte[] payload = new byte[payloadLength];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < payloadLength; ++i) {
            int anInt = random.nextInt(127);
            if (anInt == 10 || anInt == 13) {
                anInt += 1;
            }
            payload[i] = (byte) anInt;
        }

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("number", ID.incrementAndGet());
        map.put("payload", payload);
        return KiteRemoting.BytesMessage.newBuilder().setHeader(header).setBody(
                ByteString.copyFrom(JsonUtils.toJSON(map).getBytes())).build();
    }
    
    public static void main(String[] args) {
        System.setProperty("kiteq.appName", "Producer");
        new KiteProducerBenchmark(args).start();
    }
}

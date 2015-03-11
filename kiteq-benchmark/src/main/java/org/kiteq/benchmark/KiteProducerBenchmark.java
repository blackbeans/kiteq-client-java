package org.kiteq.benchmark;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.commons.util.ParamUtils;
import org.kiteq.commons.util.ThreadUtils;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaofeihang
 * @since Jan 5, 2015
 */
public class KiteProducerBenchmark {

    static final AtomicLong messageId = new AtomicLong(0);
    
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KiteProducerBenchmark.class);
    
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    private static final String TOPOIC = "trade";

    private final int threadNum;

    private KiteClient[] clients;
    
    private ExecutorService executorService;

    private final long sendInterval;
    
    public KiteProducerBenchmark(String[] args) {
        Map<String, String> params = ParamUtils.parse(args);
        String zkAddr = StringUtils.defaultString(params.get("-zkAddr"), "localhost:2181");
        System.out.println("zkAddr=" + zkAddr);
        sendInterval = NumberUtils.toLong(params.get("-sendInterval"), 1000);
        System.out.println("sendInterval=" + sendInterval);
        threadNum = NumberUtils.toInt(params.get("-t"), Runtime.getRuntime().availableProcessors() * 2);
        System.out.println("threadNum=" + threadNum);

        clients = new KiteClient[threadNum];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultKiteClient(zkAddr, new ClientConfigs(GROUP_ID, SECRET_KEY));
            clients[i].setPublishTopics(new String[] { TOPOIC });
            clients[i].start();
        }
        
        executorService = Executors.newCachedThreadPool();
    }
    
    public void start() {
        final CountDownLatch latch = new CountDownLatch(threadNum * 10);

        for (int i = 0; i < threadNum; i++) {
            final KiteClient client = clients[i];
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 10; ++j) {
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                while (!Thread.currentThread().isInterrupted()) {
                                    client.sendBytesMessage(buildMessage());

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

    private KiteRemoting.BytesMessage buildMessage() {
        String messageId = String.valueOf(KiteProducerBenchmark.messageId.getAndIncrement());
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
    
    public static void main(String[] args) {
        System.setProperty("kiteq.appName", "Producer");
        new KiteProducerBenchmark(args).start();
    }
}

package org.kiteq.benchmark;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.math.NumberUtils;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.commons.util.ParamUtils;
import org.kiteq.commons.util.ThreadUtils;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Jan 5, 2015
 */
public class KiteProducerBenchmark {
    
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KiteProducerBenchmark.class);
    
    private static final String ZK_ADDR = "localhost:2181";
    private static final String GROUP_ID = "pb-mts-test";
    private static final String SECRET_KEY = "123456";
    private static final String TOPOIC = "trade";
    
    private int threadNum = 10;
    private int loopNum = 10000 * 10000;
    
    private KiteClient[] clients;
    
    private ExecutorService executorService;
    private CountDownLatch latch = new CountDownLatch(threadNum);
    
    public KiteProducerBenchmark(String[] args) {
        
        Map<String, String> params = ParamUtils.parse(args);
        threadNum = NumberUtils.toInt(params.get("-t"), threadNum);
        
        clients = new KiteClient[threadNum];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new DefaultKiteClient(ZK_ADDR, new ClientConfigs(GROUP_ID, SECRET_KEY));
            clients[i].setPublishTopics(new String[] { TOPOIC });
            clients[i].start();
        }
        
        executorService = Executors.newCachedThreadPool();
    }
    
    public void start() {
        
        for (int i = 0; i < threadNum; i++) {
            final KiteClient client = clients[i];
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < loopNum; j++) {
                        client.sendStringMessage(buildMessage());
                    }
                    ThreadUtils.sleep(1000);
                    latch.countDown();
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
    
    private StringMessage buildMessage() {
        
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
        
        StringMessage message = StringMessage.newBuilder()
                .setHeader(header)
                .setBody("echo").build();
        
        return message;
    }
    
    public static void main(String[] args) {
        System.setProperty("kiteq.appName", "Producer");
        new KiteProducerBenchmark(args).start();
    }

}

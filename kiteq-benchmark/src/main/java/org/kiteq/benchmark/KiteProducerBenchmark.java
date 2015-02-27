package org.kiteq.benchmark;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.kiteq.client.KiteClient;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.commons.message.Message;
import org.kiteq.commons.message.StringMessage;
import org.kiteq.commons.util.ParamUtils;
import org.kiteq.commons.util.ThreadUtils;
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
            clients[i] = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY);
            clients[i].start();
        }
        
        executorService = Executors.newCachedThreadPool();
    }
    
    public void start() {
        
        for (int i = 0; i < threadNum; i++) {
            
            final KiteClient client = clients[i];
            final int threadId = i;
            
            executorService.execute(new Runnable() {
                
                @Override
                public void run() {
                    
                    for (int j = 0; j < loopNum; j++) {
                        client.sendMessage(buildMessage(threadId));
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
    
    private Message buildMessage(int threadId) {
        
        long currentTime = System.currentTimeMillis();
        
        String messageId = threadId + "" + currentTime + "" + RandomUtils.nextInt(0, Short.MAX_VALUE);
        
        StringMessage message = new StringMessage();
        message.setMessageId(messageId);
        message.setTopic("trade");
        message.setMessageType("pay-succ");
        message.setExpiredTime(currentTime);
        message.setDeliveryLimit(-1);
        message.setGroupId("go-kite-test");
        message.setCommit(true);
        message.setBody("echo");
        
        return message;
    }
    
    public static void main(String[] args) {
        new KiteProducerBenchmark(args).start();
    }

}

package org.kiteq.benchmark;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.Message;
import org.kiteq.commons.util.JsonUtils;
import org.kiteq.commons.util.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KiteConsumerBenchmark {

    private static final long EXPECT = 5000050000l; // 1 + 2 + ... + 10w

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KiteConsumerBenchmark.class);
    
    private static final String GROUP_ID = "s-mts-test";
    private static final String SECRET_KEY = "123456";

    private KiteClient[] consumers;

    private static final AtomicLong ACC = new AtomicLong(0);

    public KiteConsumerBenchmark(String[] args) {
        Map<String, String> params = ParamUtils.parse(args);
        String zkAddr = StringUtils.defaultString(params.get("-zkAddr"), "localhost:2181");
        System.out.println("zkAddr=" + zkAddr);
        int threadsNum = NumberUtils.toInt(params.get("-t"), Runtime.getRuntime().availableProcessors() * 2);
        System.out.println("threadsNum=" + threadsNum);

        consumers = new KiteClient[threadsNum];
        for (int i = 0; i < threadsNum; ++i) {
            KiteClient consumer = new DefaultKiteClient(zkAddr, new ClientConfigs(GROUP_ID, SECRET_KEY),
                    new ListenerAdapter() {
                        @Override
                        public boolean onMessage(Message message) {
                            String json = "{}";
                            if (message.isStringMessage()) {
                                json = message.getBodyString();
                            } else if (message.isBytesMessage()) {
                                json = new String(message.getBodyBytes());
                            }
                            Map<String, Object> map;
                            try {
                                map = JsonUtils.toMap(json);
                            } catch (Exception ignored) {
                                return true;
                            }
                            Object object = map.get("number");
                            if (object != null) {
                                Integer number = (Integer) object;
                                ACC.addAndGet(number);
                            }
                            return true;
                        }
                    });
            consumer.setBindings(new Binding[]{Binding.bindDirect(GROUP_ID, "trade", "pay-succ", 1000, true)});
            consumers[i] = consumer;
        }
    }
    
    public void start() {
        for (KiteClient consumer : consumers) {
            consumer.start();
        }

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println(ACC.get());
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
    
    public static void main(String[] args) {
        System.setProperty("kiteq.appName", "Consumer");
        new KiteConsumerBenchmark(args).start();
    }
}

package org.kiteq.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.kiteq.client.manager.ClientConfigs;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.Message;
import org.kiteq.commons.util.JsonUtils;
import org.kiteq.commons.util.ParamUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * luofucong at 2015-04-09.
 */
public class KiteqConsumer {

    private static final Logger LOGGER = Logger.getLogger(KiteqConsumer.class);

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("kiteq.appName", "Consumer");

        Map<String, String> params = ParamUtils.parse(args);
        String zkAddr = StringUtils.defaultString(params.get("-zkAddr"), "localhost:2181");
        LOGGER.info("zkAddr=" + zkAddr);
        final String groupId = StringUtils.defaultString(params.get("-groupId"), "s-mts-test");
        LOGGER.info("groupId=" + groupId);
        String secretKey = StringUtils.defaultString(params.get("-secretKey"), "123456");
        LOGGER.info("secretKey=" + secretKey);
        final String topic = StringUtils.defaultString(params.get("-topic"), "trade");
        LOGGER.info("topic=" + topic);
        final String messageType = StringUtils.defaultString(params.get("-messageType"), "pay-succ");
        LOGGER.info("messageType=" + messageType);
        int warmingupSec = NumberUtils.toInt(params.get("-warmingupSec"),60);
        LOGGER.info("warmingupSec=" + warmingupSec);


        final AtomicLong  count = new AtomicLong(0);
        final AtomicLong lastCount = new AtomicLong(0);
        int clientNum = 1;
        ListenerAdapter listener = new ListenerAdapter() {
            @Override
            public boolean onMessage(Message message) {
//                    LOGGER.warn(message);
                count.incrementAndGet();
                return true;
            }
        };



        new Thread(){
            @Override
            public void run() {
                while(true) {
                    long c = count.get();
                    long change = c-lastCount.get();
                    lastCount.set(c);
                    System.out.printf("%d tps\n", change);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        DefaultKiteClient[] clients = new DefaultKiteClient[clientNum];
        for (int i = 0; i < clientNum; i++) {
            clients[i] = new DefaultKiteClient();
            List<Binding> binds = new ArrayList<Binding>();
            binds.add(Binding.bindDirect(groupId, topic, messageType, 8000, true));
            clients[i].setBindings(binds);
            clients[i].setZkHosts(zkAddr);
            clients[i].setListener(listener);
            clients[i].setGroupId(groupId);
            clients[i].setSecretKey(secretKey);
            clients[i].setWarmingupSeconds(warmingupSec);
            try {
                clients[i].init();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        TimeUnit.HOURS.sleep(1);
    }
}

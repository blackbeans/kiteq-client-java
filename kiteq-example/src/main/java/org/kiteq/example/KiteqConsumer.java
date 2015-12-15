package org.kiteq.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.KiteClient;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.impl.DefaultKiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.Message;
import org.kiteq.commons.util.ParamUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        int clientNum = NumberUtils.toInt(params.get("-clients"), Runtime.getRuntime().availableProcessors() * 2);
        LOGGER.info("clientNum=" + clientNum);

        ClientConfigs clientConfigs = new ClientConfigs(groupId, secretKey);
        ListenerAdapter listener = new ListenerAdapter() {
            @Override
            public boolean onMessage(Message message) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(message);
                }
                return true;
            }
        };
        DefaultKiteClient[] clients = new DefaultKiteClient[clientNum];
        for (int i = 0; i < clientNum; i++) {
            clients[i] = new DefaultKiteClient();
            List<Binding> binds = new ArrayList<Binding>();
            binds.add(Binding.bindDirect(groupId, topic, messageType, 1000, true));
            clients[i].setBindings(binds);
            clients[i].setZkHosts(zkAddr);
            clients[i].setListener(listener);
            clients[i].setClientConfigs(clientConfigs);
            try {
                clients[i].init();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        TimeUnit.HOURS.sleep(1);
    }
}

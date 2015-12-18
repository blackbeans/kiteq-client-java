package org.kiteq.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.kiteq.client.KiteClient;
import org.kiteq.client.DefaultKiteClient;
import org.kiteq.client.message.*;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.commons.util.ParamUtils;
import org.kiteq.commons.util.ThreadUtils;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * luofucong at 2015-04-09.
 */
public class KiteqTxProducer {

    private static final Logger LOGGER = Logger.getLogger(KiteqTxProducer.class);

    private static final AtomicLong UID = new AtomicLong(0);

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("kiteq.appName", "Producer");

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
        final long sendInterval = NumberUtils.toLong(params.get("-sendInterval"), 1000);
        LOGGER.info("sendInterval=" + sendInterval);
        int clientNum = NumberUtils.toInt(params.get("-clients"), Runtime.getRuntime().availableProcessors() * 2);
        LOGGER.info("clientNum=" + clientNum);
        int workerNum = NumberUtils.toInt(params.get("-workers"), 10);
        LOGGER.info("workerNum=" + workerNum);

        ListenerAdapter listener = new ListenerAdapter() {
            @Override
            public void onMessageCheck(TxResponse response) {
                Map<String, String> properties = response.getProperties();
                String txId = properties.get("TxId");
                if (txId != null) {
                    LOGGER.info("Actively commit " + txId);
                    response.commit();
                } else {
                    LOGGER.warn("Rollback " + response);
                    response.rollback();
                }
            }
        };
        DefaultKiteClient[] clients = new DefaultKiteClient[clientNum];
        for (int i = 0; i < clientNum; i++) {
            clients[i] = new DefaultKiteClient();
            List<String> binds = new ArrayList<String>();
            binds.add(topic);
            clients[i].setPublishTopics(binds);
            clients[i].setZkHosts(zkAddr);
            clients[i].setGroupId(groupId);
            clients[i].setSecretKey( secretKey);
            clients[i].setListener(listener);
            try {
                clients[i].init();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < clientNum; i++) {
            final KiteClient client = clients[i];
            for (int j = 0; j < workerNum; ++j) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                StringMessage message =
                                        buildMessage(String.valueOf(UID.getAndIncrement()), topic, groupId, messageType);
                                client.sendTxMessage(message, new TxCallback() {
                                    @Override
                                    public void doTransaction(TxResponse txResponse) throws Exception {
                                        if (LOGGER.isDebugEnabled()) {
                                            String messageId = txResponse.getProperties().get("TxId");
                                            LOGGER.debug("handle message: " + messageId);
                                        }
                                    }
                                });
                            } catch (NoKiteqServerException e) {
                                break;
                            }

                            if (sendInterval > 0) {
                                ThreadUtils.sleep(sendInterval);
                            }
                        }
                    }
                });
            }
        }

        TimeUnit.HOURS.sleep(1);
    }

    private static StringMessage buildMessage(String messageId, String topic, String groupId, String messageType) {
        Header header = Header.newBuilder()
                .setMessageId(UUID.randomUUID().toString().replace("-", ""))
                .setTopic(topic)
                .setMessageType(messageType)
                .setExpiredTime(System.currentTimeMillis() / 1000 + TimeUnit.MINUTES.toSeconds(10))
                .setDeliverLimit(100)
                .setGroupId(groupId)
                .setCommit(false)
                .setFly(false)
                .addProperties(KiteRemoting.Entry.newBuilder().setKey("TxId").setValue(messageId)).build();
        return StringMessage.newBuilder().setHeader(header).setBody(messageId).build();
    }
}

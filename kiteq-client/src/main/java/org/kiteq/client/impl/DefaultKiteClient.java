package org.kiteq.client.impl;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.kiteq.binding.Binding;
import org.kiteq.binding.manager.BindingManager;
import org.kiteq.client.KiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.client.util.AckUtils;
import org.kiteq.client.util.MessageUtils;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.threadpool.ThreadPoolManager;
import org.kiteq.protocol.KiteRemoting.*;
import org.kiteq.protocol.Protocol;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.kiteq.remoting.listener.KiteListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class DefaultKiteClient implements KiteClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKiteClient.class);

    private static final Logger DEBUGGER_LOGGER = LoggerFactory.getLogger("debugger");
    
    private String[] publishTopics;
    private Binding[] bindings;
    
    private BindingManager bindingManager;
    private ConcurrentMap<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();

    private String groupId;
    private String secretKey;
    private MessageListener listener;

    public DefaultKiteClient(String zkAddr, String groupId, String secretKey) {
        this(zkAddr, groupId, secretKey, new ListenerAdapter() {});
    }

    public DefaultKiteClient(String zkAddr, String groupId, String secretKey, MessageListener listener) {
        this.bindingManager = BindingManager.getInstance(zkAddr);
        this.groupId = groupId;
        this.secretKey = secretKey;
        this.listener = listener;
    }
    
    @Override
    public void setPublishTopics(String[] topics) {
        this.publishTopics = topics;
    }
    
    @Override
    public void setBindings(Binding[] bindings) {
        this.bindings = bindings;
    }

    @Override
    public void start() {
        if (publishTopics != null) {
            for (String topic : publishTopics) {
                for (String serverUri : bindingManager.getServerList(topic)) {
                    createKiteQClient(topic, serverUri);
                }
            }

            String producerName = getProducerName();
            for (String topic : publishTopics) {
                bindingManager.registerProducer(topic, groupId, producerName);
            }
        }
        
        if (bindings != null) {
            for (Binding binding : bindings) {
                String topic = binding.getTopic();
                for (String serverUri : bindingManager.getServerList(topic)) {
                    createKiteQClient(topic, serverUri);
                }
            }

            bindingManager.registerConsumer(bindings);
        }
    }

    private KiteIOClient createKiteQClient(final String topic, final String serverUri) {
        KiteIOClient kiteIOClient = connMap.get(serverUri);
        if (kiteIOClient == null) {
            try {
                kiteIOClient = createKiteIOClient(serverUri);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create kiteq IO client: server=" + serverUri, e);
            }

            if (handshake(kiteIOClient)) {
                KiteIOClient _kiteIOClient;
                if ((_kiteIOClient = connMap.putIfAbsent(serverUri, kiteIOClient)) != null) {
                    kiteIOClient = _kiteIOClient;
                }
            } else {
                throw new RuntimeException(
                        "Unable to create kiteq IO client: server=" + serverUri + ", handshake refused.");
            }
        }

        kiteIOClient.getAcceptedTopics().add(topic);

        CuratorFramework curator = bindingManager.getCurator();
        try {
            final KiteIOClient finalKiteIOClient = kiteIOClient;
            curator.checkExists().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                        if (DEBUGGER_LOGGER.isDebugEnabled()) {
                            DEBUGGER_LOGGER.debug("[ZkEvents] Received " + watchedEvent);

                            DEBUGGER_LOGGER.debug("Remove topic " + topic + " from " + finalKiteIOClient);
                        }

                        finalKiteIOClient.getAcceptedTopics().remove(topic);

                        if (finalKiteIOClient.getAcceptedTopics().isEmpty()) {
                            connMap.remove(serverUri);

                            finalKiteIOClient.close();
                            logger.warn(finalKiteIOClient + " closed.");
                        }
                    }
                }
            })
                    .forPath(BindingManager.SERVER_PATH + topic + "/" + serverUri);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return kiteIOClient;
    }

    private String getProducerName() {
        String producerName;
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        if (StringUtils.isEmpty(jvmName)) {
            String hostAddress;
            try {
                hostAddress = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            producerName = hostAddress;
        } else {
            producerName = jvmName;
        }
        return producerName;
    }

    private KiteIOClient createKiteIOClient(String serverUri) throws Exception {
        
        final KiteIOClient kiteIOClient = new NettyKiteIOClient(serverUri);
        kiteIOClient.start();

        kiteIOClient.registerListener(new KiteListener() {
            
            // handle transaction response
            @Override
            public void txAckReceived(final TxACKPacket txAck) {
                final TxResponse txResponse = TxResponse.parseFrom(txAck);
                
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onMessageCheck(txResponse);
                        TxACKPacket txAckSend = txAck.toBuilder()
                                .setStatus(txResponse.getStatus()).build();
                        kiteIOClient.send(Protocol.CMD_TX_ACK, txAckSend.toByteArray());
                    }
                });
            }
            
            // handle bytes message
            @Override
            public void bytesMessageReceived(final BytesMessage message) {
                
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        if (listener.onMessage(MessageUtils.convertMessage(message))) {
                            DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader());
                            kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
                        }
                    }
                });
            }
            
            // handle string message
            @Override
            public void stringMessageReceived(final StringMessage message) {
                
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        if (listener.onMessage(MessageUtils.convertMessage(message))) {
                            DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader());
                            kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
                        }
                    }
                });
            }
        });

        return kiteIOClient;
    }

    @Override
    public void close() {
        ThreadPoolManager.shutdown();
        KiteStats.close();
        bindingManager.close();
        
        for (Entry<String, KiteIOClient> entry : connMap.entrySet()) {
            KiteIOClient kiteIOClient = entry.getValue();
            kiteIOClient.close();
        }
    }

    private boolean handshake(KiteIOClient kiteIOClient) {

        ConnMeta connMeta = ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey)
                .build();

        ConnAuthAck ack = kiteIOClient.sendAndGet(Protocol.CMD_CONN_META, connMeta.toByteArray());

        boolean status = ack.getStatus();
        logger.warn("Client handshake - serverUrl: {}, status: {}, feedback: {}", kiteIOClient.getServerUrl(), status, ack.getFeedback());

        return status;
    }

    private String selectServer(String topic) {
        List<String> serverUris = bindingManager.getServerList(topic);
        return serverUris.get(RandomUtils.nextInt(0, serverUris.size()));
    }
    
    @Override
    public SendResult sendStringMessage(StringMessage message) {
        return innerSendMessage(Protocol.CMD_STRING_MESSAGE, message.toByteArray(), message.getHeader());
    }
    
    @Override
    public SendResult sendBytesMessage(BytesMessage message) {
        return innerSendMessage(Protocol.CMD_BYTES_MESSAGE, message.toByteArray(), message.getHeader());
    }

    private SendResult innerSendMessage(byte cmdType, byte[] data, Header header) {

        String serverUrl = selectServer(header.getTopic());
        KiteIOClient kiteIOClient = connMap.get(serverUrl);
        if (kiteIOClient == null) {
            kiteIOClient = createKiteQClient(header.getTopic(), serverUrl);
        }

        SendResult result = new SendResult();

        try {
            MessageStoreAck ack = kiteIOClient.sendAndGet(cmdType, data);

            if (ack == null) {
                result.setSuccess(false);
                return result;
            }

            result.setMessageId(ack.getMessageId());
            result.setSuccess(ack.getStatus());

            if (logger.isDebugEnabled()) {
                logger.debug("Receive store ack - status: {}, feedback: {}", ack.getStatus(), ack.getFeedback());
            }
        } catch (Exception e) {
            logger.error("Send message error: {}", header, e);

            result.setMessageId(header.getMessageId());
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }

    @Override
    public void setMessageListener(MessageListener messageListener) {
        this.listener = messageListener;
    }

}

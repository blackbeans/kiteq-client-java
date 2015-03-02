package org.kiteq.client.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.RandomUtils;
import org.kiteq.binding.Binding;
import org.kiteq.binding.manager.BindingManager;
import org.kiteq.client.KiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.client.util.AckUtils;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.threadpool.ThreadPoolManager;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.ConnAuthAck;
import org.kiteq.protocol.KiteRemoting.ConnMeta;
import org.kiteq.protocol.KiteRemoting.DeliverAck;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.MessageStoreAck;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.KiteRemoting.TxACKPacket;
import org.kiteq.protocol.Protocol;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.kiteq.remoting.listener.KiteListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class DefaultKiteClient implements KiteClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKiteClient.class);
    
    private String[] publishTopics;
    private Binding[] bindings;
    
    private BindingManager bindingManager;
    private Map<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();

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
        Set<String> serverUris = new HashSet<String>();
        
        if (publishTopics != null) {
            for (String topic : publishTopics) {
                for (String serverUri : bindingManager.getServerList(topic)) {
                    serverUris.add(serverUri);
                }
            }
        }
        
        if (bindings != null) {
            for (Binding binding : bindings) {
                for (String serverUri : bindingManager.getServerList(binding.getTopic())) {
                    serverUris.add(serverUri);
                }
            }
        }
        
        for (String serverUri : serverUris) {
            try {
                KiteIOClient kiteIOClient = connMap.get(serverUri);
                if (kiteIOClient == null) {
                    synchronized (serverUri.intern()) {
                        if (kiteIOClient == null) {
                            kiteIOClient = createKiteIOClient(serverUri);
                            if (handshake(kiteIOClient)) {
                                connMap.put(serverUri, kiteIOClient);
                                logger.warn("Client connection created: {}", serverUri);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Client connection error: {}", serverUri);
            }
        }
    }
    
    private KiteIOClient createKiteIOClient(String serverUri) throws Exception {
        
        final KiteIOClient kiteIOClient = new NettyKiteIOClient(serverUri);
        kiteIOClient.start();
        
        kiteIOClient.registerListener(new KiteListener() {
            
            // handle transaction response
            @Override
            public void txAckReceived(TxACKPacket txAck) {
                final TxResponse txResponse = TxResponse.parseFrom(txAck);
                
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onMessageCheck(txResponse);
                    }
                });
                
                TxACKPacket txAckSend = txAck.toBuilder()
                        .setStatus(txResponse.getStatus()).build();
                kiteIOClient.send(Protocol.CMD_TX_ACK, txAckSend.toByteArray());
            }
            
            // handle bytes message
            @Override
            public void bytesMessageReceived(final BytesMessage message) {
                
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onBytesMessage(message);
                    }
                });
                
                DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader());
                kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
            }
            
            // handle string message
            @Override
            public void stringMessageReceived(final StringMessage message) {
                
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onStringMessage(message);
                    }
                });
                
                DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader());
                kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
            }
        });
        
        return kiteIOClient;
    }

    @Override
    public void close() {
        for (Entry<String, KiteIOClient> entry : connMap.entrySet()) {
            KiteIOClient kiteIOClient = entry.getValue();
            kiteIOClient.close();
        }
        bindingManager.close();
        KiteStats.close();
        ThreadPoolManager.shutdown();
    }

    private boolean handshake(KiteIOClient kiteIOClient) throws Exception {

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

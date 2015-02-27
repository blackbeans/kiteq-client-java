package org.kiteq.client.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.kiteq.client.KiteClient;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.MessageStatus;
import org.kiteq.client.message.SendMessageCallback;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.util.AckUtils;
import org.kiteq.client.util.MessageUtils;
import org.kiteq.commons.message.Message;
import org.kiteq.protocol.KiteRemoting.ConnAuthAck;
import org.kiteq.protocol.KiteRemoting.ConnMeta;
import org.kiteq.protocol.KiteRemoting.MessageStoreAck;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
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

    private static final String[] serverList = new String[] { "localhost:13800" };

    private Map<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();
    
    @SuppressWarnings("unused")
    private String zkAddr;
    private String groupId;
    private String secretKey;
    private MessageListener listener;

    public DefaultKiteClient(String zkAddr, String groupId, String secretKey) {
        this(zkAddr, groupId, secretKey, null);
    }
    
    public DefaultKiteClient(String zkAddr, String groupId, String secretKey, MessageListener listener) {
        this.zkAddr = zkAddr;
        this.groupId = groupId;
        this.secretKey = secretKey;
        this.listener = listener;
    }
    
    @Override
    public void start() {
        for (String serverUrl : serverList) {
            try {
                final KiteIOClient kiteIOClient = new NettyKiteIOClient(serverUrl);
                kiteIOClient.start();
                
                if (handshake(kiteIOClient)) {
                    connMap.put(serverUrl, kiteIOClient);
                    logger.warn("Client connection created: {}", serverUrl);
                }
                
                kiteIOClient.registerListener(new KiteListener() {
                    
                    @Override
                    public void packetReceived(KitePacket packet) {
                        if (listener != null) {
                            Message message = MessageUtils.unpackMessage(packet);
                            MessageStatus status = new MessageStatus();
                            listener.receiveMessage(message, status);
                            
                            KitePacket ackPacket = AckUtils.buildDeliveryAckPacket(message);
                            try {
                                kiteIOClient.send(ackPacket);
                            } catch (Exception e) {
                                logger.error("Send delivery ack error! ", e);
                            }
                        }
                    }
                });
            } catch (Exception e) {
                logger.error("Client connection error: {}", serverUrl);
            }
        }
    }

    @Override
    public void close() {
        for (Entry<String, KiteIOClient> entry : connMap.entrySet()) {
            KiteIOClient kiteIOClient = entry.getValue();
            kiteIOClient.close();
        }
    }
    
    private boolean handshake(KiteIOClient kiteIOClient) throws Exception {
        
        ConnMeta connMeta = ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey)
                .build();
        
        KitePacket request = new KitePacket(Protocol.CMD_CONN_META, connMeta.toByteArray());
        KitePacket reponse = kiteIOClient.sendAndGet(request);
        
        ConnAuthAck ack = ConnAuthAck.parseFrom(reponse.getData());
        
        boolean status = ack.getStatus();
        logger.warn("Client handshake - serverUrl: {}, status: {}, feedback: {}", kiteIOClient.getServerUrl(), status, ack.getFeedback());
        
        return status;
    }
    
    private String selectServer() {
        return serverList[0];
    }

    @Override
    public SendResult sendMessage(Message message) {
        
        String serverUrl = selectServer();
        KiteIOClient kiteIOClient = connMap.get(serverUrl);
        
        KitePacket requestMessage = MessageUtils.packMessage(message);
        SendResult result = new SendResult();
        
        try {
            KitePacket response = kiteIOClient.sendAndGet(requestMessage);
            
            if (response == null) {
                result.setSuccess(false);
                return result;
            }
            
            MessageStoreAck ack = MessageStoreAck.parseFrom(response.getData());
            
            result.setMessageId(ack.getMessageId());
            result.setSuccess(ack.getStatus());
            
            if (logger.isDebugEnabled()) {
                logger.debug("Receive store ack - status: {}, feedback: {}", ack.getStatus(), ack.getFeedback());
            }
        } catch (Exception e) {
            logger.error("Send message error: {}", message, e);
            
            result.setMessageId(message.getMessageId());
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }
        
        return result;
    }

    @Override
    public SendResult sendMessage(Message message, SendMessageCallback callback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void subscribeMessage(String groupId, String topic, String messageType) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setMessageListener(MessageListener messageListener) {
        this.listener = messageListener;
    }

}

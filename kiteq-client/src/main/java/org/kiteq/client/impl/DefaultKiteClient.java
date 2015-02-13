package org.kiteq.client.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kiteq.client.KitePublisher;
import org.kiteq.client.KiteSubscriber;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendMessageCallback;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.util.MessageUtils;
import org.kiteq.commons.message.Message;
import org.kiteq.protocol.KiteRemoting.ConnAuthAck;
import org.kiteq.protocol.KiteRemoting.ConnMeta;
import org.kiteq.protocol.KiteRemoting.MessageStoreAck;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class DefaultKiteClient implements KitePublisher, KiteSubscriber {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultKiteClient.class);

    private static final String[] serverList = new String[] { "localhost:13800" };

    private static Map<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();
    
    private String groupId;
    private String secretKey = "secretKey";

    public DefaultKiteClient(String groupId) {
        this.groupId = groupId;
        
        for (String serverUrl : serverList) {
            try {
                KiteIOClient kiteIOClient = new NettyKiteIOClient(serverUrl);
                kiteIOClient.start();
                
                if (handshake(kiteIOClient)) {
                    connMap.put(serverUrl, kiteIOClient);
                    logger.warn("client connection created: {}", serverUrl);
                }
            } catch (Exception e) {
                logger.error("client connection error: {}", serverUrl);
            }
        }
    }
    
    private boolean handshake(KiteIOClient kiteIOClient) throws Exception {
        
        ConnMeta connMeta = ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey)
                .build();
        
        KitePacket request = new KitePacket(Protocol.CMD_CONN_META, connMeta.toByteArray());
        KitePacket reponse = kiteIOClient.sendPacket(request);
        
        ConnAuthAck ack = ConnAuthAck.parseFrom(reponse.getData());
        
        boolean success = ack.getStatus();
        if (!success) {
            logger.warn("client handshake failed: {}", kiteIOClient.getServerUrl());
        }
        
        return success;
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
            KitePacket response = kiteIOClient.sendPacket(requestMessage);
            
            MessageStoreAck ack = MessageStoreAck.parseFrom(response.getData());
            
            result.setMessageId(ack.getMessageId());
            result.setSuccess(ack.getStatus());
            
            logger.warn("receive store ack - status: {}, feedback: {}", ack.getStatus(), ack.getFeedback());
        } catch (Exception e) {
            logger.error("send message error: {}", message, e);
            
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
        // TODO Auto-generated method stub

    }

}

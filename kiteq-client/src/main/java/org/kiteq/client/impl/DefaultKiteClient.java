package org.kiteq.client.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.kiteq.client.KiteClient;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.client.util.AckUtils;
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

    private static final String[] serverList = new String[] { "localhost:13800" };
    
    private Map<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();

    @SuppressWarnings("unused")
    private String zkAddr;
    private String groupId;
    private String secretKey;
    private MessageListener listener;

    public DefaultKiteClient(String zkAddr, String groupId, String secretKey) {
        this(zkAddr, groupId, secretKey, new ListenerAdapter() {});
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
                    public void txAckReceived(TxACKPacket txAck) {
                        TxResponse txResponse = TxResponse.parseFrom(txAck);
                        listener.onMessageCheck(txResponse);
                        
                        TxACKPacket txAckSend = txAck.toBuilder()
                                .setStatus(txResponse.getStatus()).build();
                        kiteIOClient.send(Protocol.CMD_TX_ACK, txAckSend.toByteArray());
                    }
                    
                    @Override
                    public void bytesMessageReceived(BytesMessage message) {
                        listener.onBytesMessage(message);
                        DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader());
                        kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
                    }
                    
                    @Override
                    public void stringMessageReceived(StringMessage message) {
                        listener.onStringMessage(message);
                        DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader());
                        kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
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

        ConnAuthAck ack = kiteIOClient.sendAndGet(Protocol.CMD_CONN_META, connMeta.toByteArray());

        boolean status = ack.getStatus();
        logger.warn("Client handshake - serverUrl: {}, status: {}, feedback: {}", kiteIOClient.getServerUrl(), status, ack.getFeedback());

        return status;
    }

    private String selectServer() {
        return serverList[0];
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

        String serverUrl = selectServer();
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

package org.kiteq.client.impl;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.kiteq.client.ClientConfigs;
import org.kiteq.client.ClientManager;
import org.kiteq.client.KiteClient;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.binding.BindingManager;
import org.kiteq.client.message.ListenerAdapter;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxCallback;
import org.kiteq.client.message.TxResponse;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.threadpool.ThreadPoolManager;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.MessageStoreAck;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.Protocol;
import org.kiteq.remoting.client.KiteIOClient;
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

    private final ClientManager clientManager;

    private final ClientConfigs clientConfigs;

    public DefaultKiteClient(String zkAddr, ClientConfigs clientConfigs) {
        this(zkAddr, clientConfigs, new ListenerAdapter() {

        });
    }

    public DefaultKiteClient(String zkAddr, ClientConfigs clientConfigs, MessageListener listener) {
        this.bindingManager = BindingManager.getInstance(zkAddr);
        this.clientConfigs = clientConfigs;

        clientManager = new ClientManager(bindingManager, clientConfigs, listener);
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
                clientManager.refreshServers(topic, bindingManager.getServerList(topic));
            }

            String producerName = getProducerName();
            for (String topic : publishTopics) {
                bindingManager.registerProducer(topic, clientConfigs.groupId, producerName);

                bindingManager.registerClientManager(topic, clientManager);
            }
        }
        
        if (bindings != null) {
            for (Binding binding : bindings) {
                String topic = binding.getTopic();
                clientManager.refreshServers(topic, bindingManager.getServerList(topic));
            }

            for (Binding binding : bindings) {
                bindingManager.registerClientManager(binding.getTopic(), clientManager);
            }

            bindingManager.registerConsumer(bindings);
        }
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

    @Override
    public void close() {
        ThreadPoolManager.shutdown();
        KiteStats.close();
        bindingManager.close();
        clientManager.close();
    }
    
    @Override
    public SendResult sendStringMessage(StringMessage message) throws NoKiteqServerException {
        return innerSendMessage(Protocol.CMD_STRING_MESSAGE, message.toByteArray(), message.getHeader());
    }
    
    @Override
    public SendResult sendBytesMessage(BytesMessage message) throws NoKiteqServerException {
        return innerSendMessage(Protocol.CMD_BYTES_MESSAGE, message.toByteArray(), message.getHeader());
    }

    @Override
    public SendResult sendTxMessage(StringMessage message, TxCallback txCallback) throws NoKiteqServerException {
        SendResult result = sendStringMessage(message);
        if (result.isSuccess()) {
            Header header = message.getHeader();
            handleTxCallback(txCallback, header);
        }
        return result;
    }

    @Override
    public SendResult sendTxMessage(BytesMessage message, TxCallback txCallback) throws NoKiteqServerException {
        SendResult result = sendBytesMessage(message);
        if (result.isSuccess()) {
            Header header = message.getHeader();
            handleTxCallback(txCallback, header);
        }
        return result;
    }

    private void handleTxCallback(TxCallback txCallback, Header header) throws NoKiteqServerException {
        TxResponse txResponse = new TxResponse(header);
        try {
            txResponse.setMessageId(header.getMessageId());

            txCallback.doTransaction(txResponse);

            txResponse.commit();
        } catch (Exception e) {
            txResponse.rollback();
            logger.warn("Rollback transaction " + header + " because of ", e);
        }

        Header.Builder _header = Header.newBuilder(header);
        _header.setCommit(true);
        Header committedHeader = _header.build();

        KiteRemoting.TxACKPacket.Builder txAck = KiteRemoting.TxACKPacket.newBuilder();
        txAck.setHeader(committedHeader);
        txAck.setStatus(txResponse.getStatus());
        txAck.setFeedback(StringUtils.defaultString(txResponse.getFeedback(), ""));
        sendMessage(Protocol.CMD_TX_ACK, txAck.build().toByteArray(), committedHeader);
    }

    private void sendMessage(byte cmdType, byte[] data, Header header) throws NoKiteqServerException {
        KiteIOClient client = clientManager.get(header.getTopic());
        client.send(cmdType, data);
    }

    private SendResult innerSendMessage(byte cmdType, byte[] data, Header header) throws NoKiteqServerException {
        SendResult result = new SendResult();
        try {
            KiteIOClient kiteIOClient = clientManager.get(header.getTopic());
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
        } catch (NoKiteqServerException ex) {
            throw ex;
        } catch (Exception e) {
            logger.error("Send message error: {}", header, e);

            result.setMessageId(header.getMessageId());
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }
}

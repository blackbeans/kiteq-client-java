package org.kiteq.client;

import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;
import org.kiteq.client.manager.ClientConfigs;
import org.kiteq.client.manager.ClientManager;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.binding.QServerManager;
import org.kiteq.client.message.*;
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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class DefaultKiteClient implements KiteClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKiteClient.class);

    private List<String> publishTopics = Collections.emptyList();
    private List<Binding> bindings = Collections.emptyList();

    private QServerManager qserverManager;

    private ClientManager clientManager;

    private ClientConfigs clientConfigs = new ClientConfigs();

    private MessageListener listener;

    private String zkHosts;

    public void setListener(MessageListener listener) {
        this.listener = listener;
    }

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }

    public String getGroupId() {
        return this.clientConfigs.getGroupId();
    }

    public void setGroupId(String groupId) {
        this.clientConfigs.setGroupId(groupId);
    }

    public void setSecretKey(String secretKey) {
        this.clientConfigs.setSecretKey(secretKey);
    }

    @Override
    public void setPublishTopics(List<String> topics) {
        this.publishTopics = topics;
    }

    @Override
    public void setBindings(List<Binding> bindings) {
        this.bindings = bindings;
    }

    @Override
    public void init() throws Exception {

        //启动Qserver
        this.qserverManager = new QServerManager();
        this.qserverManager.setZkAddr(this.zkHosts);
        this.qserverManager.init();

        //创建client的管理者
        this.clientManager = new ClientManager(qserverManager, clientConfigs, this.listener);

        //收集所有的topic
        Set<String> topics = new HashSet<String>();
        if (null != publishTopics && !publishTopics.isEmpty()) {
            //发送方这个分组信息
            this.qserverManager.publishTopics(this.getGroupId(), getProducerName(), this.publishTopics);
            topics.addAll(this.publishTopics);
        }

        if (bindings != null) {
            for (Binding binding : bindings) {
                String topic = binding.getTopic();
                topics.add(topic);
            }
        }

        //初始化kiteq的客户端管理
        this.clientManager.setTopics(topics);
        this.clientManager.init();


        //推送本地的订阅关系
        qserverManager.subscribeTopics(this.getGroupId(), bindings);

        logger.info("DefaultKiteClient|Init|SUCC|...");

    }

    private static String getProducerName() {
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
        qserverManager.destroy();
        clientManager.close();
    }

    @Override
    public SendResult sendStringMessage(StringMessage message) throws NoKiteqServerException {
        return innerSendMessage(Protocol.CMD_STRING_MESSAGE, message, message.getHeader());
    }

    @Override
    public SendResult sendBytesMessage(BytesMessage message) throws NoKiteqServerException {
        return innerSendMessage(Protocol.CMD_BYTES_MESSAGE, message, message.getHeader());
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

            if (txResponse.isRollback()) {
                logger.warn("User rollback transaction " + header);
            } else {
                txResponse.commit();
            }
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
        sendMessage(Protocol.CMD_TX_ACK, txAck.build(), committedHeader);
    }

    private void sendMessage(byte cmdType, Message message, Header header) throws NoKiteqServerException {
        KiteIOClient client = clientManager.findClient(header.getTopic());
        client.send(cmdType, message);
    }

    private SendResult innerSendMessage(byte cmdType, Message message, Header header) throws NoKiteqServerException {
        SendResult result = new SendResult();
        try {
            KiteIOClient kiteIOClient = clientManager.findClient(header.getTopic());
            MessageStoreAck ack = kiteIOClient.sendAndGet(cmdType, message);

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

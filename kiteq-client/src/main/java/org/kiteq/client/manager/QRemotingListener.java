package org.kiteq.client.manager;

import org.kiteq.client.message.Message;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.TxResponse;
import org.kiteq.client.util.AckUtils;
import org.kiteq.client.util.MessageUtils;

import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.listener.RemotingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * remoting层回调listener
 * Created by blackbeans on 12/15/15.
 */
public class QRemotingListener implements RemotingListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(QRemotingListener.class);


    private MessageListener listener;

    public QRemotingListener(MessageListener listener) {
        this.listener = listener;
    }

    @Override
    public KitePacket txAckReceived(final KitePacket packet) {
        final KiteRemoting.TxACKPacket txAck = (KiteRemoting.TxACKPacket) packet.getMessage();
        final TxResponse txResponse = TxResponse.parseFrom(txAck);

        KiteRemoting.TxACKPacket.Builder builder = txAck.toBuilder();
        try {
            listener.onMessageCheck(txResponse);
            builder.setStatus(txResponse.getStatus());
        } catch (Exception e) {
            //设置为回滚
            builder.setStatus(2);
            builder.setFeedback(e.getMessage());
        }

        KitePacket response = new KitePacket(packet.getHeader().getOpaque(), Protocol.CMD_TX_ACK, builder.build());
        return response;
    }


    @Override
    public KitePacket bytesMessageReceived(final KitePacket packet) {
        final KiteRemoting.BytesMessage message = (KiteRemoting.BytesMessage) packet.getMessage();

        return innerReceived(packet, MessageUtils.convertMessage(message));

    }

    @Override
    public KitePacket stringMessageReceived(final KitePacket packet) {
        final KiteRemoting.StringMessage message = (KiteRemoting.StringMessage) packet.getMessage();

        return innerReceived(packet, MessageUtils.convertMessage(message));
    }

    private KitePacket innerReceived(KitePacket packet, Message message) {
        boolean succ = false;
        Throwable t = null;
        try {
            succ = listener.onMessage(message);
        } catch (Throwable e) {
            LOGGER.error("bytesMessageReceived|FAIL|", e);
            succ = false;
            t =e;

        }
        KiteRemoting.DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader(), succ,t);
        KitePacket response = new KitePacket(packet.getHeader().getOpaque(), Protocol.CMD_DELIVER_ACK, ack);
        return response;
    }

}

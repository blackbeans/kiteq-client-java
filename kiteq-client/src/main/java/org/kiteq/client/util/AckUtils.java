package org.kiteq.client.util;

import org.kiteq.commons.message.Message;
import org.kiteq.protocol.KiteRemoting.DeliverAck;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 27, 2015
 */
public class AckUtils {
    
    public static KitePacket buildDeliveryAckPacket(Message message) {
        
        DeliverAck ack = DeliverAck.newBuilder()
                .setGroupId(message.getGroupId())
                .setMessageId(message.getMessageId())
                .setMessageType(message.getMessageType())
                .setTopic(message.getTopic())
                .setStatus(true)
                .build();
        
        KitePacket packet = new KitePacket(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
        
        return packet;
    }

}

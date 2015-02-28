package org.kiteq.client.util;

import org.kiteq.protocol.KiteRemoting.DeliverAck;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 27, 2015
 */
public class AckUtils {
    
    public static KitePacket buildDeliverAck(StringMessage message) {
        Header header = message.getHeader();
        DeliverAck ack = DeliverAck.newBuilder()
                .setGroupId(header.getGroupId())
                .setMessageId(header.getMessageId())
                .setMessageType(header.getMessageType())
                .setTopic(header.getTopic())
                .setStatus(true)
                .build();
        
        KitePacket packet = new KitePacket(Protocol.CMD_DELIVER_ACK, ack.toByteArray());
        return packet;
    }

}

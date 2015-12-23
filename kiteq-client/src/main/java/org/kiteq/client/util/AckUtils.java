package org.kiteq.client.util;

import org.kiteq.protocol.KiteRemoting.DeliverAck;
import org.kiteq.protocol.KiteRemoting.Header;

/**
 * @author gaofeihang
 * @since Feb 27, 2015
 */
public class AckUtils {

    public static DeliverAck buildDeliverAck(Header header, boolean succ, Throwable t) {
        DeliverAck.Builder ack = DeliverAck.newBuilder()
                .setGroupId(header.getGroupId())
                .setMessageId(header.getMessageId())
                .setMessageType(header.getMessageType())
                .setTopic(header.getTopic())
                .setStatus(succ);
        if (null != t) {
            ack.setFeedback(t.toString());
        }
        return ack.build();
    }

}

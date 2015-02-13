package org.kiteq.client.util;

import org.kiteq.commons.message.Message;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;

import com.google.protobuf.ByteString;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class MessageUtils {
    
    public static KitePacket packMessage(Message message) {
        
        byte cmdType = 0;
        byte[] data = null;
        
        Header header = buildHeader(message);
        
        if (message instanceof org.kiteq.commons.message.StringMessage) {
            
            String body = ((org.kiteq.commons.message.StringMessage) message).getBody();
            
            StringMessage stringMessage = StringMessage.newBuilder()
                    .setHeader(header)
                    .setBody(body)
                    .build();
            
            cmdType = Protocol.CMD_STRING_MESSAGE;
            data = stringMessage.toByteArray();
            
        } else if (message instanceof org.kiteq.commons.message.BytesMessage) {
            
            byte[] body = ((org.kiteq.commons.message.BytesMessage) message).getBody();
            
            BytesMessage stringMessage = BytesMessage.newBuilder()
                    .setHeader(header)
                    .setBody(ByteString.copyFrom(body))
                    .build();
            
            cmdType = Protocol.CMD_STRING_MESSAGE;
            data = stringMessage.toByteArray();
            
        } else {
            throw new IllegalArgumentException("unknown message: " + message);
        }
        
        return new KitePacket(cmdType, data);
    }
    
    private static Header buildHeader(Message message) {
        
        Header header = Header.newBuilder()
                .setMessageId(message.getMessageId())
                .setTopic(message.getTopic())
                .setMessageType(message.getMessageType())
                .setExpiredTime(message.getExpiredTime())
                .setDeliveryLimit(message.getDeliveryLimit())
                .setGroupId(message.getGroupId())
                .setCommit(message.isCommit())
                .build();
        
        return header;
    }

}

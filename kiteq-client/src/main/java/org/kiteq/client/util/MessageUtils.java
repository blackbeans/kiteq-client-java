package org.kiteq.client.util;

import org.kiteq.commons.message.Message;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.Header;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class MessageUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageUtils.class);
    
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
            
            BytesMessage bytesMessage = BytesMessage.newBuilder()
                    .setHeader(header)
                    .setBody(ByteString.copyFrom(body))
                    .build();
            
            cmdType = Protocol.CMD_STRING_MESSAGE;
            data = bytesMessage.toByteArray();
            
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
    
    public static Message unpackMessage(KitePacket packet) {
        
        byte cmdType = packet.getCmdType();
        byte[] data = packet.getData();
        
        Message message = null;
        
        if (cmdType == Protocol.CMD_STRING_MESSAGE) {
            
            try {
                StringMessage stringMessage = StringMessage.parseFrom(data);
                Header header = stringMessage.getHeader();
                
                message = new org.kiteq.commons.message.StringMessage();
                copyFromHeader(message, header);
                
                String body = stringMessage.getBody();
                
                ((org.kiteq.commons.message.StringMessage) message).setBody(body);
            } catch (Exception e) {
                logger.error("message decode error!", e);
            }
            
        } else if (cmdType == Protocol.CMD_BYTES_MESSAGE) {
            
            try {
                BytesMessage bytesMessage = BytesMessage.parseFrom(data);
                Header header = bytesMessage.getHeader();
                
                message = new org.kiteq.commons.message.BytesMessage();
                copyFromHeader(message, header);
                
                byte[] body = bytesMessage.getBody().toByteArray();
                
                ((org.kiteq.commons.message.BytesMessage) message).setBody(body);
            } catch (Exception e) {
                logger.error("message decode error!", e);
            }
        }
        
        return message;
    }
    
    private static void copyFromHeader(Message dst, Header src) {
        dst.setMessageId(src.getMessageId());
        dst.setTopic(src.getTopic());
        dst.setMessageType(src.getMessageType());
        dst.setExpiredTime(src.getExpiredTime());
        dst.setDeliveryLimit(src.getDeliveryLimit());
        dst.setGroupId(src.getGroupId());
        dst.setCommit(src.getCommit());
    }

}

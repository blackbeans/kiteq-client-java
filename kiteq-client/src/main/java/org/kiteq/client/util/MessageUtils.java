package org.kiteq.client.util;

import org.kiteq.client.message.Message;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Mar 3, 2015
 */
public class MessageUtils {
    
    public static Message convertMessage(StringMessage stringMessage) {
        Message message = new Message();
        message.setHeader(stringMessage.getHeader());
        message.setBodyType(Protocol.CMD_STRING_MESSAGE);
        message.setBodyString(stringMessage.getBody());
        return message;
    }
    
    public static Message convertMessage(BytesMessage bytesMessage) {
        Message message = new Message();
        message.setHeader(bytesMessage.getHeader());
        message.setBodyType(Protocol.CMD_BYTES_MESSAGE);
        message.setBodyBytes(bytesMessage.getBody().toByteArray());
        return message;
    }

}

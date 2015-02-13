package org.kiteq.client.message;

import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface MessageListener {
    
    void receiveMessage(Message message, MessageStatus status);

}

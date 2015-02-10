package org.kiteq.client;

import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface KiteQPublisher {
    
    void addPublishTopic(String groupId, String topic);
    
    SendResult sendMessage(Message message);
    
    SendResult sendMessage(Message message, SendMessageCallback callback);

}

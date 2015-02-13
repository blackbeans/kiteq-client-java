package org.kiteq.client;

import org.kiteq.client.message.SendMessageCallback;
import org.kiteq.client.message.SendResult;
import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface KitePublisher {
    
    void addPublishTopic(String groupId, String topic);
    
    SendResult sendMessage(Message message);
    
    SendResult sendMessage(Message message, SendMessageCallback callback);

}

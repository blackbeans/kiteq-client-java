package org.kiteq.client.impl;

import org.kiteq.client.KiteQPublisher;
import org.kiteq.client.KiteQSubscriber;
import org.kiteq.client.MessageListener;
import org.kiteq.client.SendMessageCallback;
import org.kiteq.client.SendResult;
import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class DefaultKiteQClient implements KiteQPublisher, KiteQSubscriber {

    @Override
    public void addPublishTopic(String groupId, String topic) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public SendResult sendMessage(Message message) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SendResult sendMessage(Message message, SendMessageCallback callback) {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void subscribeMessage(String groupId, String topic, String messageType) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setMessageListener(MessageListener messageListener) {
        // TODO Auto-generated method stub
        
    }

}

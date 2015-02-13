package org.kiteq.client.impl;

import org.kiteq.client.KitePublisher;
import org.kiteq.client.KiteSubscriber;
import org.kiteq.client.message.SendMessageCallback;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class DefaultKiteClient implements KitePublisher, KiteSubscriber {

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

package org.kiteq.client;

import org.kiteq.client.message.MessageListener;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface KiteSubscriber {

    void subscribeMessage(String groupId, String topic, String messageType);

    void setMessageListener(MessageListener messageListener);

}

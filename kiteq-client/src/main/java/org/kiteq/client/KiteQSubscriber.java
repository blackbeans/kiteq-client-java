package org.kiteq.client;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface KiteQSubscriber {

    void subscribeMessage(String groupId, String topic, String messageType);

    void setMessageListener(MessageListener messageListener);

}

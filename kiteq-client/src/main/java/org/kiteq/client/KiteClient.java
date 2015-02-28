package org.kiteq.client;

import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public interface KiteClient {
    
    SendResult sendStringMessage(StringMessage message);
    
    SendResult sendBytesMessage(BytesMessage message);
    
    void setMessageListener(MessageListener messageListener);
    
    void start();
    
    void close();

}

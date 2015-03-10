package org.kiteq.client;

import org.kiteq.client.binding.Binding;
import org.kiteq.client.message.SendResult;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public interface KiteClient {
    
    void setPublishTopics(String[] topics);
    
    void setBindings(Binding[] bindings);
    
    SendResult sendStringMessage(StringMessage message);
    
    SendResult sendBytesMessage(BytesMessage message);
    
    void start();
    
    void close();

}

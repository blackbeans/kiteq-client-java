package org.kiteq.remoting.client;

import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteIOClient {
    
    boolean handshake();
    
    InnerSendResult sendWithSync(Message message, long timeout);
    
    void start() throws Exception;
    
    void shutdown();

}

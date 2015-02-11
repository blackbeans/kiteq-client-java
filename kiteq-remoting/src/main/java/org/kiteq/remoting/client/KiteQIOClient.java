package org.kiteq.remoting.client;

import org.kiteq.commons.message.Message;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteQIOClient {
    
    InnerSendResult sendWithSync(Message message, long timeout);
    
    boolean isConnectted();
    
    void shutdown();

}

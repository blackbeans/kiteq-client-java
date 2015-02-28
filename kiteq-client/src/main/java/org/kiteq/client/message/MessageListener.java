package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public interface MessageListener {
    
    boolean onMessage(StringMessage message);
    
    void onMessageCheck(String messageId, TxResponse response);

}

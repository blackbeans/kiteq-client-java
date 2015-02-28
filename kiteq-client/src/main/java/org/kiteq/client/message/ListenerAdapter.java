package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public abstract class ListenerAdapter implements MessageListener {
    
    @Override
    public boolean onMessage(StringMessage message) {
        return true;
    }
    
    @Override
    public void onMessageCheck(String messageId, TxResponse response) {
    }

}

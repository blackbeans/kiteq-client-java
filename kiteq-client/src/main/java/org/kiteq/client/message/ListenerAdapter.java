package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public abstract class ListenerAdapter implements MessageListener {
    
    @Override
    public boolean onStringMessage(StringMessage message) {
        return true;
    }
    
    @Override
    public boolean onBytesMessage(BytesMessage message) {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public void onMessageCheck(TxResponse response) {
    }

}

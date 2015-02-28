package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public interface MessageListener {
    
    boolean onStringMessage(StringMessage message);
    
    boolean onBytesMessage(BytesMessage message);
    
    void onMessageCheck(TxResponse response);

}

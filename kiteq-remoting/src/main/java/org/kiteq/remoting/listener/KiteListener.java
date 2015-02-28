package org.kiteq.remoting.listener;

import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.KiteRemoting.TxACKPacket;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public interface KiteListener {
    
    void txAckReceived(TxACKPacket txAck);
    
    void bytesMessageReceived(BytesMessage message);
    
    void stringMessageReceived(StringMessage message);

}

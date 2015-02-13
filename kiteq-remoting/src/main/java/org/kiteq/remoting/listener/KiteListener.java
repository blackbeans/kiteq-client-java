package org.kiteq.remoting.listener;

import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public interface KiteListener {
    
    void packetReceived(KitePacket packet);

}

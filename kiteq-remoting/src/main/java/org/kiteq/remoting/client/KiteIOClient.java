package org.kiteq.remoting.client;

import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteIOClient {
    
    KitePacket sendPacket(KitePacket packet) throws Exception;
    
    void start() throws Exception;
    
    void close();

}

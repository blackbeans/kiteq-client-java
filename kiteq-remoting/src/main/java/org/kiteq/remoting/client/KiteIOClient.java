package org.kiteq.remoting.client;

import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.listener.KiteListener;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteIOClient {
    
    KitePacket sendPacket(KitePacket packet) throws Exception;
    
    void registerListener(KiteListener listener);
    
    void start() throws Exception;
    
    void close();
    
    String getServerUrl();

}

package org.kiteq.remoting.client;

import org.kiteq.remoting.listener.KiteListener;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteIOClient {
    
    void send(byte cmdType, byte[] data);
    
    <T> T sendAndGet(byte cmdType, byte[] data);
    
    void registerListener(KiteListener listener);
    
    void start() throws Exception;
    
    void close();
    
    String getServerUrl();

}

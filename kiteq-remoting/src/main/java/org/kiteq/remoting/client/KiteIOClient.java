package org.kiteq.remoting.client;

import com.google.protobuf.Message;
import org.kiteq.remoting.listener.KiteListener;

import java.util.Set;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteIOClient {
    
    void send(byte cmdType, Message message);

    <T> T sendAndGet(byte cmdType, Message message);
    
    void registerListener(KiteListener listener);
    
    void start() throws Exception;

    boolean reconnect();

    boolean isDead();

    void close();
    
    String getServerUrl();

    Set<String> getAcceptedTopics();

    boolean handshake();
}

package org.kiteq.remoting.client;

import com.google.protobuf.Message;
import org.kiteq.protocol.packet.KitePacket;

import java.util.Set;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public interface KiteIOClient {
    
    void send(byte cmdType, Message message);

    <T> T sendAndGet(byte cmdType, Message message);

    public void sendResponse(KitePacket packet);
    
    void start() throws Exception;

    boolean reconnect();

    boolean isDead();

    void close();
    
    String getHostPort();

    Set<String> getAcceptedTopics();

    boolean handshake();
}

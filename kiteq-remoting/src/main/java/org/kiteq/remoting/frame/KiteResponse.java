package org.kiteq.remoting.frame;

import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class KiteResponse {
    
    private String requestId;
    private KitePacket packet;
    
    public KiteResponse(long requestId, KitePacket packet) {
        this(String.valueOf(requestId), packet);
    }
    
    public KiteResponse(String requestId, KitePacket packet) {
        this.requestId = requestId;
        this.packet = packet;
    }
    
    public String getRequestId() {
        return requestId;
    }
    
    public KitePacket getPacket() {
        return packet;
    }
}

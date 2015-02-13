package org.kiteq.remoting.response;

import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class KiteResponse {
    
    private String requestId;
    private KitePacket packet;
    
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

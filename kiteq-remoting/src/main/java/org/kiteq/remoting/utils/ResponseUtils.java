package org.kiteq.remoting.utils;

import org.kiteq.protocol.KiteRemoting.ConnAuthAck;
import org.kiteq.protocol.KiteRemoting.MessageStoreAck;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public class ResponseUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(ResponseUtils.class);
    
    public static Object buildResponse(KitePacket packet) {
        
        try {
            switch (packet.getCmdType()) {
            case Protocol.CMD_CONN_AUTH:
                return ConnAuthAck.parseFrom(packet.getData());
                
            case Protocol.CMD_MESSAGE_STORE_ACK:
                return MessageStoreAck.parseFrom(packet.getData()); 

            default:
                break;
            }
        } catch (Exception e) {
            logger.error("Unpack response packet error! packet: {}", packet.toString(), e);
        }
        
        return null;
    }

}

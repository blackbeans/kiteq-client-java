package org.kiteq.remoting.client.dispatcher;

import io.netty.channel.Channel;

import org.kiteq.commons.stats.MessageStats;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.listener.KiteListener;
import org.kiteq.remoting.listener.ListenerManager;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponseFuture;
import org.kiteq.remoting.utils.ChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class KitePacketDispatcer {
    
    private static final Logger logger = LoggerFactory.getLogger(KitePacketDispatcer.class); 
    
    public static void dispatch(Channel channel, KitePacket packet) {
        
        MessageStats.recordRead();
        
        byte cmdType = packet.getCmdType();
        
        switch (cmdType) {
        case Protocol.CMD_CONN_AUTH:
        case Protocol.CMD_MESSAGE_STORE_ACK:
            receiveAck(channel, packet);
            break;
            
        case Protocol.CMD_BYTES_MESSAGE:
        case Protocol.CMD_STRING_MESSAGE:
            receiveMessage(channel, packet);
            break;

        default:
            receiveUnknownPacket(channel, packet);
            break;
        }
    }
    
    private static void receiveAck(Channel channel, KitePacket packet) {
        
        KiteResponse response = new KiteResponse(ChannelUtils.getChannelId(channel), packet);
        ResponseFuture.receiveResponse(response);
    }
    
    private static void receiveMessage(Channel channel, KitePacket packet) {
        
        KiteListener kiteListener = ListenerManager.getListener(ChannelUtils.getChannelId(channel));
        if (kiteListener != null) {
            kiteListener.packetReceived(packet);
        }
    }
    
    private static void receiveUnknownPacket(Channel channel, KitePacket packet) {
        logger.warn("unknown packet: {}", packet.toString());
    }

}

package org.kiteq.remoting.client.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.listener.KiteListener;
import org.kiteq.remoting.listener.ListenerManager;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponsFuture;
import org.kiteq.remoting.utils.ChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class KiteClientHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteClientHandler.class); 
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        
        if (msg instanceof KitePacket) {
            KitePacket packet = (KitePacket) msg;
            
            byte cmdType = packet.getCmdType();
            
            switch (cmdType) {
            case Protocol.CMD_CONN_AUTH:
            case Protocol.CMD_MESSAGE_STORE_ACK:
                receiveAck(ctx.channel(), packet);
                break;
                
            case Protocol.CMD_BYTES_MESSAGE:
            case Protocol.CMD_STRING_MESSAGE:
                receiveMessage(ctx.channel(), packet);
                break;

            default:
                break;
            }
            
            logger.debug("receive packet - cmdType: {}", packet.getCmdType());
        }
        
    }
    
    private void receiveAck(Channel channel, KitePacket packet) {
        
        KiteResponse response = new KiteResponse(ChannelUtils.getChannelId(channel), packet);
        ResponsFuture.receiveResponse(response);
    }
    
    private void receiveMessage(Channel channel, KitePacket packet) {
        
        KiteListener kiteListener = ListenerManager.getListener(ChannelUtils.getChannelId(channel));
        kiteListener.packetReceived(packet);
    }

}

package org.kiteq.remoting.client.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.client.dispatcher.KitePacketDispatcer;
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
            
            KitePacketDispatcer.dispatch(ctx.channel(), packet);
            
            logger.debug("receive packet - cmdType: {}", packet.getCmdType());
        } else {
            logger.warn("Illegal message {}", msg);
        }
    }
    
}

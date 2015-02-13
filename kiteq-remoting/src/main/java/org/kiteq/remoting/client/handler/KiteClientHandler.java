package org.kiteq.remoting.client.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.frame.KiteResponse;
import org.kiteq.remoting.frame.ResponsFuture;
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
            
            Channel channel = ctx.channel();
            KiteResponse response = new KiteResponse(channel.hashCode(), packet);
            ResponsFuture.receiveResponse(response);
            
            logger.debug("receive packet - cmdType: {}", packet.getCmdType());
        }
        
    }

}

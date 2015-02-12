package org.kiteq.remoting.client.handler;

import org.kiteq.protocol.KiteRemoting.ConnAuthAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyClientHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class); 
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        
        if (msg instanceof ConnAuthAck) {
            ConnAuthAck connAuthAck = (ConnAuthAck) msg;
            logger.warn("Conn ack received: {}", connAuthAck.toString());
        }
        
    }

}

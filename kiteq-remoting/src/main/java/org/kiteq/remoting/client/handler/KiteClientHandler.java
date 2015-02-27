package org.kiteq.remoting.client.handler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    
    
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        
        if (msg instanceof KitePacket) {
            final KitePacket packet = (KitePacket) msg;
            
//            executorService.execute(new Runnable() {
//                
//                @Override
//                public void run() {
//                    
//                }
//            });
            
            KitePacketDispatcer.dispatch(ctx.channel(), packet);
            
            if (logger.isDebugEnabled()) {
                logger.debug("receive packet - cmdType: {}", packet.getCmdType());
            }
        } else {
            logger.warn("Illegal message {}", msg);
        }
    }
    
}

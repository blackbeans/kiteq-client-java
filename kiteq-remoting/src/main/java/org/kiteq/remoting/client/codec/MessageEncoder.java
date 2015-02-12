package org.kiteq.remoting.client.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.kiteq.protocol.KiteRemoting.ConnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 5, 2015
 */
public class MessageEncoder extends MessageToMessageEncoder<Object> {
    
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);
    
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        
        byte[] src = null;
        
        if (msg instanceof ConnMeta) {
            ConnMeta connMeta = (ConnMeta) msg;
            src = connMeta.toByteArray();
        }
        
        if (src != null) {
            ByteBuf buf = ctx.alloc().buffer(src.length + 2);
            buf.writeBytes(src);
            buf.writeBytes(new byte[] { '\r', '\n' });
            out.add(buf);
        }
    }

}

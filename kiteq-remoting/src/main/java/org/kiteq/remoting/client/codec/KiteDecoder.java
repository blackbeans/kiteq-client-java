package org.kiteq.remoting.client.codec;

import org.kiteq.protocol.KiteRemoting.ConnAuthAck;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;

/**
 * @author gaofeihang
 * @since Feb 5, 2015
 */
public class KiteDecoder extends DelimiterBasedFrameDecoder {
    
    private static final int MAX_LENGTH = Integer.MAX_VALUE;
    private static final ByteBuf[] DELIMITERS = Delimiters.lineDelimiter();

    public KiteDecoder() {
        super(MAX_LENGTH, DELIMITERS);
    }
    
    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        
        ByteBuf buf = (ByteBuf) super.decode(ctx, buffer);
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        buf.release();
        
        ConnAuthAck connAuthAck = ConnAuthAck.parseFrom(data);
        
        return connAuthAck;
    }

}

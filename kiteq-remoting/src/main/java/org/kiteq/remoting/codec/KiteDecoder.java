package org.kiteq.remoting.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.kiteq.commons.util.ByteArrayUtils;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.utils.ByteBufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 5, 2015
 */
public class KiteDecoder extends LengthFieldBasedFrameDecoder {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteDecoder.class);
    
    private static final int MAX_LENGTH = Integer.MAX_VALUE;

    public KiteDecoder() {
        super(MAX_LENGTH, 5, 4);
    }
    
    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        
        logger.debug("receive hex: {}", ByteArrayUtils.prettyPrint(ByteBufUtils.toByteArray(in)));
        
        skipCLRF(in);
        
        ByteBuf buf = (ByteBuf) super.decode(ctx, in);
        
        if (buf == null) {
            return buf;
        }
        
        return KitePacket.parseFrom(buf);
    }
    
    private void skipCLRF(ByteBuf buffer) {
        
        if (buffer.readableBytes() < 2) {
            return;
        }
        
        buffer.markReaderIndex();
        byte[] skipBytes = new byte[2];
        buffer.readBytes(skipBytes);
        
        if (skipBytes[0] != '\r' || skipBytes[1] != '\n') {
            buffer.resetReaderIndex();
        }
    }

}

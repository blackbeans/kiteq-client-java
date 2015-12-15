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

    public KiteDecoder() {
        super(32 * 1024, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("receive hex: {}", ByteArrayUtils.prettyPrint(ByteBufUtils.toByteArray(in)));
        }

        ByteBuf buf = (ByteBuf) super.decode(ctx, in);


        if (buf == null) {
            return buf;
        }

        try {
            return KitePacket.parseFrom(buf);
        } finally {
            buf.release();
        }
    }


}

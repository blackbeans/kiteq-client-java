package org.kiteq.remoting.utils;

import io.netty.buffer.ByteBuf;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class ByteBufUtils {
    
    public static byte[] toByteArray(ByteBuf buf) {
        ByteBuf copyBuf = buf.duplicate();
        byte[] dst = new byte[copyBuf.readableBytes()];
        copyBuf.readBytes(dst);
        return dst;
    }

}

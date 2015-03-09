package org.kiteq.protocol.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.kiteq.commons.util.ByteArrayUtils;
import org.kiteq.protocol.Protocol;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class KitePacket {

    private static final AtomicInteger UNIQUE_ID = new AtomicInteger(0);

    private int opaque;
    private byte cmdType;
    private byte[] data;
    
    public KitePacket(byte cmdType, byte[] data) {
        this.opaque = getPacketId();
        this.cmdType = cmdType;
        this.data = data;
    }

    private KitePacket(int opaque, byte cmdType, byte[] data) {
        this(cmdType, data);
        this.opaque = opaque;
    }

    private int getPacketId() {
        int id = UNIQUE_ID.getAndIncrement();
        if (id == Integer.MAX_VALUE) {
            UNIQUE_ID.compareAndSet(Integer.MAX_VALUE, 0);
            return UNIQUE_ID.getAndIncrement();
        }
        return id;
    }
    
    public int getOpaque() {
        return opaque;
    }
    
    public byte getCmdType() {
        return cmdType;
    }
    
    public byte[] getData() {
        return data;
    }
    
    public ByteBuf toByteBuf() {

        int length = Protocol.PACKET_HEAD_LEN + data.length + 2;
        ByteBuf buf = Unpooled.buffer(length);
        
        buf.writeInt(opaque); // 4 byte
        buf.writeByte(cmdType); // 1 byte
        buf.writeInt(data.length); // 4 byte
        buf.writeBytes(data);
        buf.writeBytes(Protocol.CMD_STR_CRLF); // \r\n
        
        return buf;
    }
    
    public static KitePacket parseFrom(ByteBuf buf) {
        
        int opaque = buf.readInt();
        byte cmdType = buf.readByte();
        int length = buf.readInt();
        byte[] data = new byte[length];
        buf.readBytes(data);
        
        return new KitePacket(opaque, cmdType, data);
    }
    
    @Override
    public String toString() {
        return cmdType + ":" + ByteArrayUtils.prettyPrint(data);
    }

}

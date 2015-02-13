package org.kiteq.protocol.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.kiteq.protocol.Protocol;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class KitePacket {
    
    private int opaque;
    private byte cmdType;
    private byte[] data;
    
    public KitePacket(byte cmdType, byte[] data) {
        this.opaque = -1;
        this.cmdType = cmdType;
        this.data = data;
    }
    
    public KitePacket(int opaque, byte cmdType, byte[] data) {
        this(cmdType, data);
        this.opaque = opaque;
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

}

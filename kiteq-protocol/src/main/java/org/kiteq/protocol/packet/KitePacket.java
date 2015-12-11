package org.kiteq.protocol.packet;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.log4j.Logger;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.Protocol;

import java.lang.ref.WeakReference;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class KitePacket {

    private static final Logger LOGGER = Logger.getLogger(KitePacket.class);

    private static ThreadLocal<WeakReference<byte[]>> buffer =
            new ThreadLocal<WeakReference<byte[]>>() {
                @Override
                protected WeakReference<byte[]> initialValue() {
                    return new WeakReference<byte[]>(new byte[4096]);
                }
            };

    private KitePacketHeader header;

    private final Message message;

    public KitePacket(byte cmdType, Message message) {
        this.header = new KitePacketHeader(cmdType);
        this.message = message;
    }

    public KitePacket(int opaque, byte cmdType, Message message) {
        this.header = new KitePacketHeader(opaque, cmdType);
        this.message = message;
    }

    public KitePacket(int opaque, byte cmdType, short version, long extension, Message message) {
        this.header = new KitePacketHeader(opaque, cmdType, version, extension);
        this.message = message;
    }

    public KitePacketHeader getHeader() {
        return header;
    }

    public void setHeader(KitePacketHeader header) {
        this.header = header;
    }

    public Message getMessage() {
        return message;
    }

    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        byte[] data = message.toByteArray();
        int length = 4 + Protocol.PACKET_HEAD_LEN + 4 + data.length;
        ByteBuf buf = allocator.directBuffer(length);
        buf.writeInt(length); // 总长度
        buf.writeInt(header.getOpaque()); // 4 byte
        buf.writeByte(header.getCmdType()); // 1 byte
        buf.writeShort(header.getVersion());    // 2 byte
        buf.writeLong(header.getExtension());   // 8 byte
        buf.writeInt(data.length); // 4 byte body长度
        buf.writeBytes(data);
        return buf;
    }

    public static KitePacket parseFrom(ByteBuf buf) throws Exception {
        buf.readInt(); // 总长度
        int opaque = buf.readInt();
        byte cmdType = buf.readByte();
        short version = buf.readShort(); // version 2 byte
        long extension = buf.readLong();    // extension 8 byte

        int bodyLength = buf.readInt(); // read body length
        if (buf.readableBytes() < bodyLength) {
            throw new Exception("Kiteq client decode error: incorrect data length!");
        }
        Message msg = null;
        byte[] arr;
        int off;
        int len = buf.readableBytes();
        if (buf.hasArray()) {
            arr = buf.array();
            off = buf.readerIndex();
        } else {
            byte[] bytes = buffer.get().get();
            if (bytes == null) {
                bytes = new byte[32 * 1024];
                buffer.set(new WeakReference<byte[]>(bytes));
            }
            if (bytes.length < len) {
                bytes = new byte[len];
                buffer.set(new WeakReference<byte[]>(bytes));
            }
            arr = bytes;
            buf.getBytes(buf.readerIndex(), arr, 0, len);
            off = 0;
        }
        try {
            switch (cmdType) {
                case Protocol.CMD_CONN_AUTH:
                    msg = KiteRemoting.ConnAuthAck.getDefaultInstance().getParserForType().parseFrom(arr, off, len);
                    break;
                case Protocol.CMD_MESSAGE_STORE_ACK:
                    msg = KiteRemoting.MessageStoreAck.getDefaultInstance().getParserForType().parseFrom(arr, off, len);
                    break;
                case Protocol.CMD_TX_ACK:
                    msg = KiteRemoting.TxACKPacket.getDefaultInstance().getParserForType().parseFrom(arr, off, len);
                    break;
                case Protocol.CMD_BYTES_MESSAGE:
                    msg = KiteRemoting.BytesMessage.getDefaultInstance().getParserForType().parseFrom(arr, off, len);
                    break;
                case Protocol.CMD_STRING_MESSAGE:
                    msg = KiteRemoting.StringMessage.getDefaultInstance().getParserForType().parseFrom(arr, off, len);
                    break;
                case Protocol.CMD_HEARTBEAT:
                    msg = KiteRemoting.HeartBeat.getDefaultInstance().getParserForType().parseFrom(arr, off, len);
                    break;
                default:
                    LOGGER.warn("Received unknown msg type: " + cmdType);
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("msg type: " + cmdType, e);
        }
        return new KitePacket(opaque, cmdType, version, extension, msg);
    }

    @Override
    public String toString() {
        return "KitePacket{" +
                "header=" + header +
                ", message=" + message +
                '}';
    }
}

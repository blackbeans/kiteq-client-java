package org.kiteq.protocol.packet;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.log4j.Logger;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.Protocol;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class KitePacket {

    private static final Logger LOGGER = Logger.getLogger(KitePacket.class);

    private static final AtomicInteger UNIQUE_ID = new AtomicInteger(0);

    private static ThreadLocal<WeakReference<byte[]>> buffer =
            new ThreadLocal<WeakReference<byte[]>>() {
                @Override
                protected WeakReference<byte[]> initialValue() {
                    return new WeakReference<byte[]>(new byte[4096]);
                }
            };

    private int opaque;
    private byte cmdType;

    private final Message message;

    public KitePacket(byte cmdType, Message message) {
        this.opaque = getPacketId();
        this.cmdType = cmdType;
        this.message = message;
    }

    private KitePacket(int opaque, byte cmdType, Message message) {
        this(cmdType, message);
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

    public Message getMessage() {
        return message;
    }

    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        byte[] data = message.toByteArray();
        int length = Protocol.PACKET_HEAD_LEN + data.length + 2;
        ByteBuf buf = allocator.directBuffer(length);
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
        buf.readInt(); // read length

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
                bytes = new byte[4096];
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
        return new KitePacket(opaque, cmdType, msg);
    }
    
    @Override
    public String toString() {
        return cmdType + ":" + message;
    }
}

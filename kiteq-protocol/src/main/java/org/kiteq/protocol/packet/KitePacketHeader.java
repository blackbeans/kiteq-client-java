package org.kiteq.protocol.packet;

import org.kiteq.protocol.Protocol;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: easynoder
 * Date: 15/12/11
 * Time: 下午5:02
 * Desc:
 */
public class KitePacketHeader {

    private static final AtomicLong UNIQUE_ID = new AtomicLong(0);

    private static final int MAX_OPAQUE_ID = 50 * 10000;

    private int opaque;
    private byte cmdType;
    private short version;
    private long extension;

    public KitePacketHeader(byte cmdType) {
        this(getPacketId(), cmdType);
    }

    public KitePacketHeader(int opaque, byte cmdType) {
        this(opaque, cmdType, Protocol.HEAD_VERSION, Protocol.HEAD_EXTENSION);
    }

    public KitePacketHeader(int opaque, byte cmdType, short version, long extension) {
        this.opaque = opaque;
        this.cmdType = cmdType;
        this.version = version;
        this.extension = extension;
    }

    private static int getPacketId() {
        return (int)(UNIQUE_ID.getAndIncrement() % MAX_OPAQUE_ID);
    }

    public long getExtension() {
        return extension;
    }

    public void setExtension(long extension) {
        this.extension = extension;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public byte getCmdType() {
        return cmdType;
    }

    public void setCmdType(byte cmdType) {
        this.cmdType = cmdType;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "KitePacketHeader{" +
                "opaque=" + opaque +
                ", cmdType=" + cmdType +
                ", version=" + version +
                ", extension=" + extension +
                '}';
    }
}

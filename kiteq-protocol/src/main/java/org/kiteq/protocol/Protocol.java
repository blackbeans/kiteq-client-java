package org.kiteq.protocol;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class Protocol {

    public static final byte CMD_HEARTBEAT = 0x01;
    public static final byte CMD_CONN_META = 0x02;
    public static final byte CMD_CONN_AUTH = 0x03;

    public static final byte CMD_MESSAGE_STORE_ACK = 0x04;
    public static final byte CMD_DELIVER_ACK = 0x05;
    public static final byte CMD_TX_ACK = 0x06;

    public static final byte CMD_BYTES_MESSAGE = 0x11;
    public static final byte CMD_STRING_MESSAGE = 0x12;

    public static final byte[] CMD_STR_CRLF = new byte[]{'\r', '\n'};

    public static final int PACKET_HEAD_LEN = 4 + 1 + 2 + 8 ;

    public static final int TX_UNKNOWN = 0;
    public static final int TX_COMMIT = 1;
    public static final int TX_ROLLBACK = 2;


    public static final long HEAD_EXTENSION = 0l;
    public static final short HEAD_VERSION = 2; // 基于长度


}

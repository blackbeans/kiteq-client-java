package org.kiteq.remoting.client.dispatcher;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.Channel;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.*;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.listener.KiteListener;
import org.kiteq.remoting.listener.ListenerManager;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponseFuture;
import org.kiteq.remoting.utils.ChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class KitePacketDispatcer {
    
    private static final Logger logger = LoggerFactory.getLogger(KitePacketDispatcer.class); 
    
    public static void dispatch(Channel channel, KitePacket packet) {
        
        KiteStats.recordRead();
        
        byte cmdType = packet.getCmdType();

        try {
            switch (cmdType) {
            case Protocol.CMD_CONN_AUTH:
                receiveConnAuth(packet);
                break;
                
            case Protocol.CMD_MESSAGE_STORE_ACK:
                receiveMessageStoreAck(packet);
                break;
            
            case Protocol.CMD_TX_ACK:
                receiveTxAck(channel, packet);
                break;
            
            case Protocol.CMD_BYTES_MESSAGE:
                receiveBytesMessage(channel, packet);
                break;
                
            case Protocol.CMD_STRING_MESSAGE:
                receiveStringMessage(channel, packet);
                break;
                case Protocol.CMD_HEARTBEAT:
                    receiveHeartbeatResp(packet);
                    break;
            default:
                receiveUnknownPacket(channel, packet);
                break;
            }
        } catch (Exception e) {
            logger.error("Kite packet dispatcher error! packet: {}", packet.toString());
        }
    }

    private static KiteResponse buildKiteResponse(KitePacket packet, Object model) {
        return new KiteResponse(packet.getOpaque(), model);
    }

    private static KiteListener getKiteListener(Channel channel) {
        return ListenerManager.getListener(ChannelUtils.getChannelId(channel));
    }

    private static void receiveConnAuth(KitePacket packet) throws InvalidProtocolBufferException {
        ConnAuthAck connAuthAck = ConnAuthAck.parseFrom(packet.getData());
        ResponseFuture.receiveResponse(buildKiteResponse(packet, connAuthAck));
    }

    private static void receiveMessageStoreAck(KitePacket packet) throws InvalidProtocolBufferException {
        MessageStoreAck messageStoreAck = MessageStoreAck.parseFrom(packet.getData());
        ResponseFuture.receiveResponse(buildKiteResponse(packet, messageStoreAck));
    }
    
    private static void receiveTxAck(Channel channel, KitePacket packet) throws InvalidProtocolBufferException {
        TxACKPacket txAck = TxACKPacket.parseFrom(packet.getData());
        getKiteListener(channel).txAckReceived(txAck);
    }
    
    private static void receiveBytesMessage(Channel channel, KitePacket packet) throws InvalidProtocolBufferException {
        BytesMessage message = BytesMessage.parseFrom(packet.getData());
        getKiteListener(channel).bytesMessageReceived(message);
    }
    
    private static void receiveStringMessage(Channel channel, KitePacket packet) throws InvalidProtocolBufferException {
        StringMessage message = StringMessage.parseFrom(packet.getData());
        getKiteListener(channel).stringMessageReceived(message);
    }

    private static void receiveHeartbeatResp(KitePacket packet) throws InvalidProtocolBufferException {
        KiteRemoting.HeartBeat heartBeat = KiteRemoting.HeartBeat.parseFrom(packet.getData());
        ResponseFuture.receiveResponse(buildKiteResponse(packet, heartBeat));
    }

    private static void receiveUnknownPacket(Channel channel, KitePacket packet) {
        logger.warn("unknown packet: {}", packet.toString());
    }

}

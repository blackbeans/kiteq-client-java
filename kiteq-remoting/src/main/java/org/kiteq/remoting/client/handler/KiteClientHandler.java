package org.kiteq.remoting.client.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.protocol.KiteRemoting;
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
 * @since Feb 11, 2015
 */
public class KiteClientHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(KiteClientHandler.class);
    
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        
        if (msg instanceof KitePacket) {
            KiteStats.recordRead();

            KitePacket packet = (KitePacket) msg;
            byte cmdType = packet.getHeader().getCmdType();
            if (cmdType == Protocol.CMD_CONN_AUTH ||
                    cmdType == Protocol.CMD_MESSAGE_STORE_ACK ||
                    cmdType == Protocol.CMD_HEARTBEAT) {
                ResponseFuture.receiveResponse(new KiteResponse(packet.getHeader().getOpaque(), packet.getMessage()));
            } else {
                KiteListener listener = ListenerManager.getListener(ChannelUtils.getChannelId(ctx.channel()));
                if (cmdType == Protocol.CMD_TX_ACK) {
                    listener.txAckReceived(packet);
                } else if (cmdType == Protocol.CMD_BYTES_MESSAGE) {
                    listener.bytesMessageReceived( packet);
                } else if (cmdType == Protocol.CMD_STRING_MESSAGE) {
                    listener.stringMessageReceived(packet);
                } else {
                    logger.error("Received unknown package: " + packet);
                }
            }
        } else {
            logger.warn("Illegal message {}", msg);
        }
    }
}

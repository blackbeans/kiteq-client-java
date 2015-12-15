package org.kiteq.remoting.client.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.threadpool.ThreadPoolManager;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.listener.RemotingListener;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponseFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class KiteClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(KiteClientHandler.class);

    private RemotingListener remotingListener;

    public KiteClientHandler(RemotingListener remotingListener) {
        this.remotingListener = remotingListener;
    }



    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {

        ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
            @Override
            public void run() {
                if (msg instanceof KitePacket) {
                    KiteStats.recordRead();

                    KitePacket packet = (KitePacket) msg;
                    byte cmdType = packet.getHeader().getCmdType();
                    if (cmdType == Protocol.CMD_CONN_AUTH ||
                            cmdType == Protocol.CMD_MESSAGE_STORE_ACK ||
                            cmdType == Protocol.CMD_HEARTBEAT) {
                        ResponseFuture.receiveResponse(new KiteResponse(packet.getHeader().getOpaque(), packet.getMessage()));
                    } else {
                        KitePacket response = null;
                        if (cmdType == Protocol.CMD_TX_ACK) {
                            response = KiteClientHandler.this.remotingListener.txAckReceived(packet);
                        } else if (cmdType == Protocol.CMD_BYTES_MESSAGE) {
                            response = KiteClientHandler.this.remotingListener.bytesMessageReceived(packet);
                        } else if (cmdType == Protocol.CMD_STRING_MESSAGE) {
                            response = KiteClientHandler.this.remotingListener.stringMessageReceived(packet);
                        } else {
                            logger.error("Received unknown package: " + packet);
                        }

                        if (null != response) {
                            //发送回执
                            ctx.channel().writeAndFlush(response);
                        }
                    }
                } else {
                    logger.warn("Illegal message {}", msg);
                }
            }
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }
}

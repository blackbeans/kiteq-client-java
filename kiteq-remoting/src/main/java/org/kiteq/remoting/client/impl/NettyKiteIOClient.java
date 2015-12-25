package org.kiteq.remoting.client.impl;

import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.util.HostPort;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.handler.KiteClientHandler;
import org.kiteq.remoting.codec.KiteDecoder;
import org.kiteq.remoting.codec.KiteEncoder;
import org.kiteq.remoting.listener.RemotingListener;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponseFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
 * ioclient的管理
 * Created by blackbeans on 12/15/15.
 */
public class NettyKiteIOClient implements KiteIOClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyKiteIOClient.class);

    private final String groupId;

    private final String secretKey;

    private String serverUrl;

    private HostPort hostPort;

    private Bootstrap bootstrap;

    private EventLoopGroup workerGroup;

    private volatile ChannelFuture channelFuture;

    private volatile AtomicBoolean alive = new AtomicBoolean(false);

    private RemotingListener listener;

    //重连次数
    private int retryCount = 0;

    public NettyKiteIOClient(String groupId, String secretKey, String serverUrl, RemotingListener listener) {
        this.groupId = groupId;
        this.secretKey = secretKey;
        this.serverUrl = serverUrl;
        this.listener = listener;
        this.hostPort = HostPort.parse(serverUrl.split("\\?")[0]);

    }

    @Override
    public void start() throws Exception {


        this.workerGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(workerGroup);
        this.bootstrap.channel(NioSocketChannel.class);
        this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60 * 1000);//60s连接超时
        this.bootstrap.option(ChannelOption.SO_TIMEOUT, 1);
        this.bootstrap.option(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        this.bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        this.bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("heartbeat",new IdleStateHandler(30,30,30, TimeUnit.SECONDS));
                ch.pipeline().addLast("encoder", new KiteEncoder());
                ch.pipeline().addLast("decoder", new KiteDecoder());
                ch.pipeline().addLast("kiteq-handler", new KiteClientHandler(listener,alive));
            }
        });

        channelFuture = bootstrap.connect(hostPort.getHost(), hostPort.getPort()).sync();

        //握手
        if (this.handshake()) {
            this.alive.compareAndSet(false,true);
        }
    }

    @Override
    public boolean reconnect() {

        LOGGER.info("{}|reconnecting start|{}...", this.hostPort, retryCount);
        ChannelFuture future = null;
        try {
            future = bootstrap.connect(hostPort.getHost(), hostPort.getPort()).sync();
        } catch (Exception e) {
            LOGGER.error("reconnect|" + this.hostPort + "|FAIL", e);
        }
        if (null != future && future.isSuccess()) {
            this.channelFuture = future;
            //尝试建立握手
            if (handshake()) {
                this.alive.compareAndSet(false,true);
                this.retryCount=0;
                LOGGER.info(this.hostPort+"|reconnecting succ..." );
                return true;
            } else {
                //如果握手失败则关掉了解
                future.channel().close();
            }
        }
        LOGGER.info(this.hostPort+"|reconnecting fail|wait for next ...|"+retryCount);
        this.retryCount++;
        return false;
    }

    @Override
    public boolean isDead() {
        return !this.alive.get();
    }

    @Override
    public void close() {
        this.alive.compareAndSet(true,false);
        this.channelFuture.channel().close();
        workerGroup.shutdownGracefully();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T sendAndGet(byte cmdType, Message message) {
        final KitePacket reqPacket = new KitePacket(cmdType, message);
        ResponseFuture future = new ResponseFuture(reqPacket.getHeader().getOpaque());

        Channel channel = channelFuture.channel();
        ChannelFuture writeFuture = channel.write(reqPacket);
        writeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    KiteStats.recordWrite();
                } else {
                    LOGGER.error("Unable to send packet: " + reqPacket, future.cause());
                }
            }
        });
        channel.flush();

        try {
            KiteResponse response = future.get(3, TimeUnit.SECONDS);
            if (response == null) {
                LOGGER.warn("Request timeout, null response received - request: {}", reqPacket.toString());
                return null;
            }

            @SuppressWarnings("unchecked")
            T model = (T) response.getModel();
            return model;
        } catch (InterruptedException ex) {
            LOGGER.error("Ops", ex);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error("get kite response error!", e);
        }
        return null;
    }


    @Override
    public void send(byte cmdType, Message message) {
        KitePacket reqPacket = new KitePacket(cmdType, message);

        Channel channel = channelFuture.channel();
        ChannelFuture writeFuture = channel.write(reqPacket);
        writeFuture.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    KiteStats.recordWrite();
                } else {
                    LOGGER.error("write message fail!", future.cause());
                }
            }
        });
        channel.flush();
    }

    @Override
    public void sendResponse(KitePacket packet) {

        Channel channel = channelFuture.channel();
        ChannelFuture writeFuture = channel.writeAndFlush(packet);
        writeFuture.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    KiteStats.recordWrite();
                } else {
                    LOGGER.error("write message fail!", future.cause());
                }
            }
        });
        channel.flush();
    }


    @Override
    public String getHostPort() {
        return serverUrl;
    }


    @Override
    public boolean handshake() {
        KiteRemoting.ConnMeta connMeta = KiteRemoting.ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey)
                .build();

        KiteRemoting.ConnAuthAck ack = sendAndGet(Protocol.CMD_CONN_META, connMeta);

        boolean success = ack.getStatus();
        LOGGER.info("Client handshake - serverUrl: {}, status: {}, feedback: {}",
                serverUrl, success, ack.getFeedback());
        return success;
    }

    @Override
    public String toString() {
        return "NettyKiteIOClient{" +
                "groupId='" + groupId + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", serverUrl='" + serverUrl + '\'' +
                ", hostPort=" + hostPort +
                ", alive=" + alive +
                ", retryCount=" + retryCount +
                '}';
    }

    @Override
    public int getReconnectCount() {
        return this.retryCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NettyKiteIOClient that = (NettyKiteIOClient) o;

        if (groupId != null ? !groupId.equals(that.groupId) : that.groupId != null) return false;
        if (secretKey != null ? !secretKey.equals(that.secretKey) : that.secretKey != null) return false;
        if (serverUrl != null ? !serverUrl.equals(that.serverUrl) : that.serverUrl != null) return false;
        return !(hostPort != null ? !hostPort.equals(that.hostPort) : that.hostPort != null);

    }

    @Override
    public int hashCode() {
        int result = groupId != null ? groupId.hashCode() : 0;
        result = 31 * result + (secretKey != null ? secretKey.hashCode() : 0);
        result = 31 * result + (serverUrl != null ? serverUrl.hashCode() : 0);
        result = 31 * result + (hostPort != null ? hostPort.hashCode() : 0);
        return result;
    }
}

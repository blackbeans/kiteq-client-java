package org.kiteq.remoting.client.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.util.HostPort;
import org.kiteq.commons.util.NamedThreadFactory;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.handler.KiteClientHandler;
import org.kiteq.remoting.codec.KiteDecoder;
import org.kiteq.remoting.codec.KiteEncoder;
import org.kiteq.remoting.listener.KiteListener;
import org.kiteq.remoting.listener.ListenerManager;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponseFuture;
import org.kiteq.remoting.utils.ChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyKiteIOClient implements KiteIOClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyKiteIOClient.class);

    private static final Logger DEBUGGER_LOGGER = LoggerFactory.getLogger("debugger");

    private String serverUrl;
    
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    private Set<String> acceptTopics = Collections.synchronizedSet(new HashSet<String>());

    private final AtomicInteger heartbeatStopCount = new AtomicInteger(0);

    private ScheduledExecutorService heartbeatExecutor;

    public NettyKiteIOClient(String serverUrl) {
        this.serverUrl = serverUrl;
    }
    
    @Override
    public void start() throws Exception {
        HostPort hostPort = HostPort.parse(serverUrl.split("\\?")[0]);
        
        workerGroup = new NioEventLoopGroup();
        
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new KiteEncoder());
                ch.pipeline().addLast(new KiteDecoder());
                ch.pipeline().addLast(new KiteClientHandler());
            }
        });

        channelFuture = bootstrap
                .connect(hostPort.getHost(), hostPort.getPort()).sync();
        
        KiteStats.start();

        startHeartbeat();
    }

    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("HeartBeat-" + serverUrl));
        heartbeatExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long version = System.currentTimeMillis();
                KiteRemoting.HeartBeat heartBeat = KiteRemoting.HeartBeat.newBuilder()
                        .setVersion(version).build();
                KiteRemoting.HeartBeat response = sendAndGet(Protocol.CMD_HEARTBEAT, heartBeat.toByteArray());
                if (response != null && response.getVersion() == version) {
                    heartbeatStopCount.set(0);
                } else {
                    heartbeatStopCount.incrementAndGet();
                }

                if (DEBUGGER_LOGGER.isDebugEnabled()) {
                    DEBUGGER_LOGGER.debug("Send heartbeat: " + heartBeat + "," +
                            " response: " + response + "," +
                            " heartbeatStopCount: " + heartbeatStopCount.get());
                }
            }
        }, 1, 2, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stopHeartBeating();
            }
        }));
    }

    @Override
    public boolean isDead() {
        return heartbeatStopCount.get() >= 2;
    }
    
    @Override
    public void close() {
        workerGroup.shutdownGracefully();

        stopHeartBeating();
    }

    private void stopHeartBeating() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T sendAndGet(byte cmdType, byte[] data) {
        final KitePacket reqPacket = new KitePacket(cmdType, data);
        ResponseFuture future = new ResponseFuture(reqPacket.getOpaque());

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
            KiteResponse response = future.get(1, TimeUnit.SECONDS);
            if (response == null) {
                LOGGER.warn("Request timeout, null response received - request: {}", reqPacket.toString());
                return null;
            }
            @SuppressWarnings("unchecked")
            T model = (T) response.getModel();
            return model;
        } catch (Exception e) {
            LOGGER.error("get kite response error!", e);
        }
        return null;
    }

    @Override
    public void send(byte cmdType, byte[] data) {
        
        KitePacket reqPacket = new KitePacket(cmdType, data);
        
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
    public void registerListener(KiteListener listener) {
        Channel channel = channelFuture.channel();
        ListenerManager.register(ChannelUtils.getChannelId(channel), listener);
    }

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Set<String> getAcceptedTopics() {
        return acceptTopics;
    }

    @Override
    public String toString() {
        return "NettyKiteIOClient{" +
                "serverUrl='" + serverUrl + '\'' +
                ", acceptTopics=" + acceptTopics +
                '}';
    }
}

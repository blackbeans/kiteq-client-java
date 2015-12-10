package org.kiteq.remoting.client.impl;

import com.google.common.collect.MapMaker;
import com.google.protobuf.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyKiteIOClient implements KiteIOClient {

    public enum STATE {
        NONE,
        PREPARE, // handshake
        RUNNING, RECONNECTING, RECOVERING, STOP
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyKiteIOClient.class);

    private static final Logger DEBUGGER_LOGGER = LoggerFactory.getLogger("debugger");

    private final String groupId;

    private final String secretKey;

    private String serverUrl;

    private final HostPort hostPort;

    private final Bootstrap bootstrap;

    private final EventLoopGroup workerGroup;

    private volatile ChannelFuture channelFuture;

    private Set<String> acceptTopics = Collections.synchronizedSet(new HashSet<String>());

    private final Heartbeat heartbeat = new Heartbeat();

    private final AtomicReference<STATE> state = new AtomicReference<STATE>(STATE.NONE);

    private final ConcurrentMap<KiteListener, Boolean> listeners = new MapMaker().weakKeys().makeMap();

    public NettyKiteIOClient(String groupId, String secretKey, String serverUrl) {
        this.groupId = groupId;
        this.secretKey = secretKey;
        this.serverUrl = serverUrl;
        hostPort = HostPort.parse(serverUrl.split("\\?")[0]);
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_TIMEOUT,1);
        bootstrap.option(ChannelOption.TCP_NODELAY,true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new KiteEncoder());
                ch.pipeline().addLast(new KiteDecoder());
                ch.pipeline().addLast(new KiteClientHandler());
            }
        });
    }
    
    @Override
    public void start() throws Exception {
        if (!state.compareAndSet(STATE.NONE, STATE.PREPARE)) {
            return;
        }

        channelFuture = bootstrap.connect(hostPort.getHost(), hostPort.getPort()).sync();

        KiteStats.start();

        heartbeat.start();
    }

    @Override
    public boolean reconnect() {
        if (!state.compareAndSet(STATE.RUNNING, STATE.RECONNECTING)) {
            return false;
        }

        String oldChannel = ChannelUtils.getChannelId(channelFuture.channel());

        int retryCount = 1;
        while (!Thread.currentThread().isInterrupted()) {
            ChannelFuture future = reconnect0(retryCount++);

            if (state.get() == STATE.RECOVERING) {
                channelFuture = future;

                heartbeat.reset();

                String newChannel = ChannelUtils.getChannelId(channelFuture.channel());
                for (KiteListener listener : listeners.keySet()) {
                    ListenerManager.register(newChannel, listener);
                }
                ListenerManager.unregister(oldChannel);

                if (handshake()) {
                    state.set(STATE.RUNNING);
                } else {
                    LOGGER.warn(this + " reconnecting error: Handshake refused!");
                    return false;
                }
                LOGGER.info(this + " reconnecting success");
                return true;
            } else if (retryCount > 10 || state.get() == STATE.STOP) {
                LOGGER.warn(this + " reconnecting error!");
                return false;
            }
        }
        LOGGER.warn(this + " reconnecting error: Interrupted");
        return false;
    }

    private ChannelFuture reconnect0(final int retryCount) {
        if (DEBUGGER_LOGGER.isDebugEnabled()) {
            DEBUGGER_LOGGER.debug(this + " reconnecting retry count " + retryCount);
        }
        ChannelFuture future = null;
        try {
            future = bootstrap.connect(hostPort.getHost(), hostPort.getPort()).addListener(
                    new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                state.set(STATE.RECOVERING);
                            }
                        }
                    });
        } catch (RejectedExecutionException ignored) {
            // looks like some one else has close the channel externally
            // for instance, the zookeeper watcher
        }
        try {
            Thread.sleep(retryCount * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return future;
    }

    @Override
    public boolean isDead() {
        return heartbeat.stopCount.get() >= 5;
    }
    
    @Override
    public void close() {
        workerGroup.shutdownGracefully();

        heartbeat.stop();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T sendAndGet(byte cmdType, Message message) {
        final KitePacket reqPacket = new KitePacket(cmdType, message);
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
            } else {
                heartbeat.setNextHeartbeatTime(System.currentTimeMillis() + 2000);
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
    public void registerListener(KiteListener listener) {
        listeners.putIfAbsent(listener, true);

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
    public boolean handshake() {
        KiteRemoting.ConnMeta connMeta = KiteRemoting.ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey)
                .build();

        KiteRemoting.ConnAuthAck ack = sendAndGet(Protocol.CMD_CONN_META, connMeta);

        boolean success = ack.getStatus();
        LOGGER.info("Client handshake - serverUrl: {}, status: {}, feedback: {}",
                serverUrl, success, ack.getFeedback());
        if (success) {
            state.compareAndSet(STATE.PREPARE, STATE.RUNNING);
        }
        return success;
    }

    @Override
    public String toString() {
        return "NettyKiteIOClient{" +
                "serverUrl='" + serverUrl + '\'' +
                ", acceptTopics=" + acceptTopics +
                ", heartbeatStopCount=" + heartbeat.stopCount.get() +
                ", state=" + state +
                '}';
    }

    private class Heartbeat {

        final AtomicInteger stopCount = new AtomicInteger(0);

        final AtomicLong nextHeartbeatTime = new AtomicLong();

        ExecutorService heartbeatExecutor;

        void start() {
            heartbeatExecutor = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("HeartBeat-" + serverUrl));
            heartbeatExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted()) {
                        if (state.get() == STATE.RUNNING) {
                            long now = System.currentTimeMillis();
                            long _nextHeartBeatTime = nextHeartbeatTime.get();
                            if (now > _nextHeartBeatTime) {
                                KiteRemoting.HeartBeat heartBeat = KiteRemoting.HeartBeat.newBuilder()
                                        .setVersion(now).build();
                                KiteRemoting.HeartBeat response = sendAndGet(Protocol.CMD_HEARTBEAT, heartBeat);
                                if (response != null && response.getVersion() == now) {
                                    stopCount.set(0);
                                } else {
                                    stopCount.incrementAndGet();
                                }

                                if (DEBUGGER_LOGGER.isDebugEnabled()) {
                                    DEBUGGER_LOGGER.debug("Send heartbeat: " + heartBeat + "," +
                                            " response: " + response + "," +
                                            " heartbeatStopCount: " + stopCount.get());
                                }
                            } else {
                                try {
                                    Thread.sleep(_nextHeartBeatTime - now);
                                } catch (InterruptedException e) {
                                    return;
                                }
                            }
                            continue;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    stop();
                }
            }));
        }

        void setNextHeartbeatTime(long time) {
            long next = nextHeartbeatTime.get();
            if (time > next) {
                if (!nextHeartbeatTime.compareAndSet(next, time)) {
                    setNextHeartbeatTime(time);
                }
            }
        }

        void reset() {
            stopCount.set(0);
        }

        void stop() {
            if (heartbeatExecutor != null) {
                heartbeatExecutor.shutdownNow();
            }
        }
    }
}

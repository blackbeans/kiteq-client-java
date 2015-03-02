package org.kiteq.remoting.client.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.TimeUnit;

import org.kiteq.commons.stats.KiteStats;
import org.kiteq.commons.util.HostPort;
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

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyKiteIOClient implements KiteIOClient {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyKiteIOClient.class);
    
    private String serverUrl;
    
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    
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
    }
    
    @Override
    public void close() {
        workerGroup.shutdownGracefully();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T sendAndGet(byte cmdType, byte[] data) {
        
        KitePacket reqPacket = new KitePacket(cmdType, data);
        
        Channel channel = channelFuture.channel();
        String requestId = ChannelUtils.getChannelId(channel);
        
        ResponseFuture future = new ResponseFuture(requestId);
        
        ChannelFuture writeFuture = channel.write(reqPacket);
        writeFuture.addListener(new ChannelFutureListener() {
            
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    KiteStats.recordWrite();
                } else {
                    logger.error("write message fail!", future.cause());
                }
            }
        });
        channel.flush();
        
        try {
            KiteResponse response = future.get(1, TimeUnit.SECONDS);
            
            if (response == null) {
                logger.warn("Request timeout, null response received - request: {}", reqPacket.toString());
                return null;
            }
            
            return (T) response.getModel();
        } catch (Exception e) {
            logger.error("get kite response error!", e);
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
                    logger.error("write message fail!", future.cause());
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

}

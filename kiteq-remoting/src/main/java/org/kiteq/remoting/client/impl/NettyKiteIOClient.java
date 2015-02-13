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

import org.kiteq.commons.util.HostPort;
import org.kiteq.protocol.packet.KitePacket;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.handler.KiteClientHandler;
import org.kiteq.remoting.codec.KiteDecoder;
import org.kiteq.remoting.codec.KiteEncoder;
import org.kiteq.remoting.listener.KiteListener;
import org.kiteq.remoting.listener.ListenerManager;
import org.kiteq.remoting.response.KiteResponse;
import org.kiteq.remoting.response.ResponsFuture;
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
    }
    
    @Override
    public void close() {
        channelFuture.channel().close();
        workerGroup.shutdownGracefully();
    }
    
    @Override
    public KitePacket sendPacket(KitePacket reqPacket) {
        
        Channel channel = channelFuture.channel();
        ResponsFuture future = new ResponsFuture(ChannelUtils.getChannelId(channel));
        
        ChannelFuture writeFuture = channel.write(reqPacket);
        
        writeFuture.addListener(new ChannelFutureListener() {
            
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("write message fail!", future.cause());
                }
            }
        });
        channel.flush();
        
        try {
            KiteResponse response = future.get();
            return response.getPacket();
        } catch (Exception e) {
            logger.error("get kite response error!", e);
        }
        
        return null;
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

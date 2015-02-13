package org.kiteq.remoting.client.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.kiteq.commons.message.Message;
import org.kiteq.commons.util.HostPort;
import org.kiteq.protocol.KiteRemoting.ConnMeta;
import org.kiteq.remoting.client.InnerSendResult;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.handler.KiteClientHandler;
import org.kiteq.remoting.frame.ResponsFuture;
import org.kiteq.remoting.frame.KiteResponse;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyKiteIOClient implements KiteIOClient {
    
    
    private String groupId;
    private String serverUrl;
    private String secretKey = "secretKey";
    
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    
    public NettyKiteIOClient(String groupId, String serverUrl) {
        this.groupId = groupId;
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
                ch.pipeline().addLast(new KiteClientHandler());
            }
        });

        channelFuture = bootstrap
                .connect(hostPort.getHost(), hostPort.getPort()).sync();
    }
    
    @Override
    public void shutdown() {
        workerGroup.shutdownGracefully();
    }
    
    @Override
    public boolean handshake() {
        
        ConnMeta connMeta = ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey).build();
        
        Channel channel = channelFuture.channel();
        
        ResponsFuture future = new ResponsFuture(channel.hashCode());
        
        channel.write(connMeta);
        channel.flush();
        
        
        return false;
    }

    @Override
    public InnerSendResult sendWithSync(Message message, long timeout) {
        channelFuture.channel().write(message);
        return null;
    }

}

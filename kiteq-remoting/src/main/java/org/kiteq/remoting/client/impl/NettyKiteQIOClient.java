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
import org.kiteq.remoting.client.KiteQIOClient;
import org.kiteq.remoting.client.handler.NettyClientHandler;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyKiteQIOClient implements KiteQIOClient {
    
    private String groupId;
    private String secretKey = "secretKey";
    
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    
    public NettyKiteQIOClient(String groupId, String serverUrl) throws Exception {
        this.groupId = groupId;
        
        HostPort hostPort = HostPort.parse(serverUrl.split("\\?")[0]);
        
        workerGroup = new NioEventLoopGroup();
        
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new NettyClientHandler());
            }
        });

        channelFuture = bootstrap
                .connect(hostPort.getHost(), hostPort.getPort()).sync();
    }
    
    @Override
    public boolean handshake() {
        
        ConnMeta connMeta = ConnMeta.newBuilder()
                .setGroupId(groupId)
                .setSecretKey(secretKey).build();
        
        Channel channel = channelFuture.channel();
        channel.write(connMeta);
        channel.flush();
        
        return false;
    }

    @Override
    public InnerSendResult sendWithSync(Message message, long timeout) {
        channelFuture.channel().write(message);
        return null;
    }

    @Override
    public boolean isConnectted() {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public void shutdown() {
        workerGroup.shutdownGracefully();
    }

}

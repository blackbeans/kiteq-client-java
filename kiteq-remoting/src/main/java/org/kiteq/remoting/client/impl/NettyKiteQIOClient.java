package org.kiteq.remoting.client.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.kiteq.commons.message.Message;
import org.kiteq.commons.util.HostPort;
import org.kiteq.remoting.client.InnerSendResult;
import org.kiteq.remoting.client.KiteQIOClient;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class NettyKiteQIOClient implements KiteQIOClient {
    
    public NettyKiteQIOClient(String serverUrl) throws Exception {
        
        HostPort hostPort = HostPort.parse(serverUrl.split("\\?")[0]);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new TimeClientHandler());
                }
            });

            ChannelFuture f = b.connect(hostPort.getHost(), hostPort.getPort()).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public InnerSendResult sendWithSync(Message message, long timeout) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isConnectted() {
        // TODO Auto-generated method stub
        return false;
    }

}

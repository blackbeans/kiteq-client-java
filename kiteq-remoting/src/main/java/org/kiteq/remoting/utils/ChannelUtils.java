package org.kiteq.remoting.utils;

import io.netty.channel.Channel;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class ChannelUtils {
    
    public static String getChannelId(Channel channel) {
        return String.valueOf(channel.hashCode());
    }
    
}

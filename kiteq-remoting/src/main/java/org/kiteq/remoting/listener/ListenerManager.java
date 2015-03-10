package org.kiteq.remoting.listener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;
import org.kiteq.protocol.KiteRemoting.TxACKPacket;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public class ListenerManager {
    
    public static Map<String, KiteListener> listeners = new ConcurrentHashMap<String, KiteListener>();
    
    public static void register(String channel, KiteListener listener) {
        listeners.put(channel, listener);
    }
    
    public static void unregister(String channel) {
        listeners.remove(channel);
    }
    
    public static KiteListener getListener(String channel) {
        KiteListener listener = listeners.get(channel);
        return listener == null ? defaultListener : listener;
    }

    private static KiteListener defaultListener = new KiteListener() {
        
        @Override
        public void txAckReceived(TxACKPacket txAck) {
        }
        
        @Override
        public void stringMessageReceived(StringMessage message) {
        }
        
        @Override
        public void bytesMessageReceived(BytesMessage message) {
        }
    };
}

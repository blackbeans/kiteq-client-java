package org.kiteq.remoting.listener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        return listeners.get(channel);
    }

}

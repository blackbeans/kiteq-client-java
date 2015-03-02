package org.kiteq.binding.manager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Mar 2, 2015
 */
public class BindingManager {
    
    private static final Logger logger = LoggerFactory.getLogger(BindingManager.class);
    
    private static final String SERVER_PATH = "/kiteq/server/";
    
    private static Map<String, BindingManager> instances = new ConcurrentHashMap<String, BindingManager>();
    
    private CuratorFramework curatorClient;
    private Map<String, List<String>> topicServerMap = new ConcurrentHashMap<String, List<String>>();
    
    public static BindingManager getInstance(String zkAddr) {
        BindingManager bindingManager = instances.get(zkAddr);
        if (bindingManager == null) {
            synchronized (zkAddr.intern()) {
                bindingManager = instances.get(zkAddr);
                if (bindingManager == null) {
                    bindingManager = new BindingManager(zkAddr);
                    instances.put(zkAddr, bindingManager);
                }
            }
        }
        return bindingManager;
    }
    
    private BindingManager(String zkAddr) {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorClient = CuratorFrameworkFactory.newClient(zkAddr, retryPolicy);
        curatorClient.start();
    }
    
    public List<String> getServerList(String topic) {
        List<String> serverUris = topicServerMap.get(topic);
        if (serverUris == null) {
            synchronized (topic.intern()) {
                serverUris = topicServerMap.get(topic);
                if (serverUris == null) {
                    try {
                        serverUris = curatorClient.getChildren().forPath(SERVER_PATH + topic);
                        topicServerMap.put(topic, serverUris);
                    } catch (Exception e) {
                        logger.error("get server list error! topic: {}", topic, e);
                    }
                }
            }
        }
        return serverUris;
    }
    
    public void close() {
        curatorClient.close();
    }

}

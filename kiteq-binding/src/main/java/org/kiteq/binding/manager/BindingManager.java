package org.kiteq.binding.manager;

import java.util.Collections;
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
    private Map<String, String[]> topicServerMap = new ConcurrentHashMap<String, String[]>();
    
    public static BindingManager getInstance(String zkAddr) {
        BindingManager bindingManager = instances.get(zkAddr);
        if (bindingManager == null) {
            synchronized (zkAddr.intern()) {
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
    
    @SuppressWarnings("unchecked")
    public List<String> getServerList(String topic) {
        String[] serverList = topicServerMap.get(topic);
        if (serverList == null) {
            try {
                List<String> serverUris = curatorClient.getChildren().forPath(SERVER_PATH + topic);
                return serverUris;
            } catch (Exception e) {
                logger.error("get server list error! topic: {}", topic, e);
            }
        }
        return Collections.EMPTY_LIST;
    }
    
    public void close() {
        curatorClient.close();
    }

}

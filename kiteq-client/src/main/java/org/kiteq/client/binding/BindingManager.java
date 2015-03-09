package org.kiteq.client.binding;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.kiteq.commons.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gaofeihang
 * @since Mar 2, 2015
 */
public class BindingManager {
    
    private static final Logger logger = LoggerFactory.getLogger(BindingManager.class);

    private static final Logger DEBUGGER_LOGGER = LoggerFactory.getLogger("debugger");

    public static final String SERVER_PATH = "/kiteq/server/";

    private static final String PRODUCER_ZK_PATH = "/kiteq/pub";

    private static final String CONSUMER_ZK_PATH = "/kiteq/sub";
    
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

    public List<String> getServerList(final String topic) {
        List<String> serverUris = topicServerMap.get(topic);
        if (serverUris == null) {
            synchronized (topic.intern()) {
                serverUris = topicServerMap.get(topic);
                if (serverUris == null) {
                    try {
                        CuratorWatcher serversChangeWatcher = new CuratorWatcher() {
                            @Override
                            public void process(WatchedEvent watchedEvent) throws Exception {
                                if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                    List<String> newServersUris = curatorClient.getChildren()
                                            .usingWatcher(this).forPath(SERVER_PATH + topic);

                                    if (DEBUGGER_LOGGER.isDebugEnabled()) {
                                        DEBUGGER_LOGGER.debug("[ZkEvents] Received " + watchedEvent);

                                        DEBUGGER_LOGGER.debug("Replace servers "
                                                + JsonUtils.toJSON(topicServerMap.get(topic))
                                                + " ->" + JsonUtils.toJSON(newServersUris));
                                    }

                                    topicServerMap.put(topic, newServersUris);
                                }
                            }
                        };
                        serverUris = curatorClient.getChildren()
                                .usingWatcher(serversChangeWatcher).forPath(SERVER_PATH + topic);
                        topicServerMap.put(topic, serverUris);
                    } catch (Exception e) {
                        logger.error("get server list error! topic: {}", topic, e);
                    }
                }
            }
        }
        return serverUris;
    }

    public void registerProducer(String topic, String groupId, String producerName) {
        String path = PRODUCER_ZK_PATH + "/" + topic + "/" + groupId + "/" + producerName;
        try {
            curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void registerConsumer(Binding[] bindings) {
        Map<Pair<String, String>, List<Binding>> bindingsMap = new HashMap<Pair<String, String>, List<Binding>>();
        for (Binding binding : bindings) {
            Pair<String, String> bindingPair = Pair.of(binding.getTopic(), binding.getGroupId());
            List<Binding> _bindings = bindingsMap.get(bindingPair);
            if (_bindings == null) {
                _bindings = new ArrayList<Binding>(1);
                _bindings.add(binding);
                bindingsMap.put(bindingPair, _bindings);
            } else {
                _bindings.add(binding);
            }
        }

        for (Map.Entry<Pair<String, String>, List<Binding>> bindingEntry : bindingsMap.entrySet()) {
            Pair<String, String> bindingPair = bindingEntry.getKey();
            String path = CONSUMER_ZK_PATH + "/" + bindingPair.getLeft() + "/" + bindingPair.getRight() + "-bind";

            boolean nodeExisted = false;
            try {
                nodeExisted = curatorClient.checkExists().forPath(path) != null;
            } catch (KeeperException.NoNodeException ignored) {
                logger.warn("Ignored.", ignored);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            byte[] data = JsonUtils.toJSON(bindingEntry.getValue()).getBytes();
            try {
                if (nodeExisted) {
                    curatorClient.setData().forPath(path, data);
                } else {
                    try {
                        curatorClient.create().creatingParentsIfNeeded().forPath(path, data);
                    } catch (KeeperException.NodeExistsException ignored) {
                        logger.warn("Ignored.", ignored);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        curatorClient.close();
    }

    public CuratorFramework getCurator() {
        return curatorClient;
    }
}

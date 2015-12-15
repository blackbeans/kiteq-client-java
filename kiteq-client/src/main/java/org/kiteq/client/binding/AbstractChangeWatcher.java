package org.kiteq.client.binding;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 变化监听器监听器
 *
 */
public abstract class AbstractChangeWatcher implements CuratorWatcher{

    private static final Logger logger = LoggerFactory.getLogger(AbstractChangeWatcher.class);

    protected CuratorFramework zkClient;

    public void setZkClient(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        switch (watchedEvent.getType()){
            case NodeChildrenChanged:
                List<String> nodes =this.zkClient.getChildren().forPath(watchedEvent.getPath());
                String topic = watchedEvent.getPath().substring(watchedEvent.getPath().lastIndexOf("/") + 1);
                this.qServerNodeChange(topic,nodes);
                logger.info("NodeChildrenChanged|"+watchedEvent.getPath(),nodes);
                break;
            case NodeDeleted:
                //ignored
                break;
            case NodeCreated:
                break;
            //ignore
            case NodeDataChanged:
                break;
            //ignore
        }

        //add watcher
      this.zkClient.checkExists().usingWatcher(this);
    }


    /**
     * qserver节点变更
     * @param topic
     * @param address
     */
    protected abstract void qServerNodeChange(String topic,List<String> address);


}
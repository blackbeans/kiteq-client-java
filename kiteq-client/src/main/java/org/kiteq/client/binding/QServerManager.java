package org.kiteq.client.binding;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.kiteq.commons.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 *
 * Qmanager的管理
 * Created by blackbeans on 12/15/15.
 */
public class QServerManager {

    private static final Logger logger = LoggerFactory.getLogger(QServerManager.class);

    public static final String PATH_SERVER = "/kiteq/server";

    private static final String PATH_PRODUCER = "/kiteq/pub";

    private static final String PATH_CONSUMER = "/kiteq/sub";

    private CuratorFramework zkClient;
    private String  zkAddr;


    public void init() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkAddr, retryPolicy);
        zkClient.start();
    }

    public void setZkAddr(String zkAddr) {
        this.zkAddr = zkAddr;
    }

    public CuratorFramework getZkClient() {
        return zkClient;
    }

    /**
     * 通过topic获取QServer
     *KITEQ_SERVER        = KITEQ + "/server" // 临时节点 # /kiteq/server/${topic}/ip:port
     ** @param topic
     * @return
     */
    public List<String> pullAndWatchQServer(String topic, final AbstractChangeWatcher watcher) {
        String path = PATH_SERVER + "/" + topic;
        List<String> children = Collections.EMPTY_LIST;
        try {
            children = this.zkClient.getChildren().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            logger.error("pullAndWatchQServer|FAIL|" + path, e);
        }
        return children;
    }


    /**
     * //    KITEQ_PUB  = KITEQ + "/pub"    //# /kiteq/pub/${topic}/${groupId}/ip:port
     * 发布本地的发送者发送的消息类型
     *
     * @param group
     * @param topics
     * @param publisherTag
     * @throws Exception
     */
    public void publishTopics(String group, String publisherTag, List<String> topics) throws Exception {
        for (String topic : topics) {
            String path = PATH_PRODUCER + "/" + topic +"/"+group +"/" + publisherTag;
            if(null !=this.zkClient.checkExists().forPath(path)) {
                //先删除再推送临时节点
                this.zkClient.delete().forPath(path);
            }
            String eppath = this.zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
            logger.info("publishTopics|SUCC|" + eppath);
        }
    }


    /**
     * KITEQ_SUB           = KITEQ + "/sub"    // 持久订阅/或者临时订阅 # /kiteq/sub/${topic}/${groupId}-bind/#$data(bind)
     * 发布本地的发送者发送的消息类型
     *
     * @param group
     * @param binds
     * @throws Exception
     */
    public void subscribeTopics(String group, List<Binding> binds) throws Exception {
        Map<String, List<Binding>> topics2Binds = new HashMap<String, List<Binding>>();
        for (Binding bind : binds) {
            bind.setGroupId(group);
            if (topics2Binds.containsKey(bind.getTopic())) {
                topics2Binds.get(bind.getTopic()).add(bind);
            } else {
                List<Binding> tmp = new ArrayList<Binding>();
                tmp.add(bind);
                topics2Binds.put(bind.getTopic(), tmp);
            }
        }

        //按照topic推送订阅关系
        for (Map.Entry<String, List<Binding>> entry : topics2Binds.entrySet()) {
            //开始推送订阅关系
            String path = PATH_CONSUMER + "/" + entry.getKey() + "/" + group + "-bind";
            Stat stat = this.zkClient.checkExists().forPath(path);
            if(null == stat){
                this.zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            }
            this.zkClient.setData().forPath(path, JsonUtils.toJSON(entry.getValue()).getBytes("UTF-8"));
            logger.info("subscribeTopics|Subscribe|SUCC|" + path + "|" + JsonUtils.toJSON(entry.getValue()));
        }
    }


    public void destroy() {
        zkClient.close();
    }

}

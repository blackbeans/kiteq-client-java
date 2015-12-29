package org.kiteq.client.manager;

import org.apache.commons.lang3.RandomUtils;
import org.kiteq.client.binding.AbstractChangeWatcher;
import org.kiteq.client.binding.QServerManager;
import org.kiteq.client.message.MessageListener;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.kiteq.remoting.listener.RemotingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * luofucong at 2015-03-05.
 */
public class ClientManager extends AbstractChangeWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);


    private final ConcurrentMap<String, List<KiteIOClient>> topic2Servers = new ConcurrentHashMap<String, List<KiteIOClient>>();

    //hostport对应的服务器列表地址
    private final ConcurrentMap<String/*hostport*/, FutureTask<KiteIOClient>> hostport2Server
            = new ConcurrentHashMap<String, FutureTask<KiteIOClient>>();

    //hostport对应的topic列表
    private final ConcurrentMap<String/*hostport*/, Set<String>> hostport2Topics =
            new ConcurrentHashMap<String, Set<String>>();

    //当前支持的所有的topic列表
    private Set<String> topics = Collections.emptySet();

    private final QServerManager qserverManager;

    //重连任务
    private ReconnectManager reconnectManager;

    private final ClientConfigs clientConfigs;

    private final QRemotingListener listener;

    //最大重连次数
    private final int maxReconTimes = 30;

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    //重连回调方法
    private ReconnectManager.IReconnectCallback callback = new ReconnectManager.IReconnectCallback() {


        @Override
        public void callback(boolean succ, KiteIOClient client) {
            synchronized (ClientManager.this.lock) {
                //重连成功并且host到topic的对应关系中存在当前client的地址,则增加
                if (succ && ClientManager.this.hostport2Topics.containsKey(client.getHostPort())) {
                    //如果成功需要恢复所有topic中该client
                    ConcurrentMap<String/*hostport*/, Set<String>> tmp = ClientManager.this.hostport2Topics;
                    Set<String> topics = tmp.get(client.getHostPort());
                    for (String topic : topics) {
                        List<KiteIOClient> clients = ClientManager.this.topic2Servers.get(topic);
                        if (null == clients) {
                            clients = new ArrayList<KiteIOClient>();
                            List<KiteIOClient> exist = ClientManager.this.topic2Servers.putIfAbsent(topic, clients);
                            if (null != exist) {
                                clients = exist;
                            }
                        }

                        for (KiteIOClient c : clients) {
                            //如果已经存在则放弃
                            if (c.getHostPort().equalsIgnoreCase(client.getHostPort())) {
                                break;
                            }
                            //添加该client
                            clients.add(client);
                            //推送当前的发送方该分组IP
                        }
                    }
                } else {

                    try {
                        ClientManager.this.hostport2Server.remove(client.getHostPort());
                        ClientManager.this.hostport2Topics.remove(client.getHostPort());

                        //直接删除对应这个连接下的的所有topic的对应关系
                        ConcurrentMap<String/*hostport*/, Set<String>> tmp = ClientManager.this.hostport2Topics;
                        Set<String> topics = tmp.get(client.getHostPort());
                        for (String topic : topics) {
                            List<KiteIOClient> clients = ClientManager.this.topic2Servers.get(topic);
                            if (null != clients) {
                                clients.remove(client);
                            }
                        }
                    } finally {
                        //总要关闭的
                        client.close();
                    }

                }
            }
        }
    };

    public ClientManager(QServerManager qServerManager, ClientConfigs clientConfigs, MessageListener listener) {
        this.qserverManager = qServerManager;
        this.clientConfigs = clientConfigs;
        this.listener = new QRemotingListener(listener);
    }


    public void init() throws Exception {
        super.zkClient = this.qserverManager.getZkClient();
        //设置所有的topic列表
        Set<String> serverList = new HashSet<String>();
        Map<String, Set<String>> topic2Server = new HashMap<String, Set<String>>();
        for (String topic : topics) {
            //获取对应的kiteq的server
            List<String> kiteq = this.qserverManager.pullAndWatchQServer(topic, this);
            if (null != kiteq && !kiteq.isEmpty()) {
                topic2Server.put(topic, new ConcurrentSkipListSet<String>(kiteq));
                serverList.addAll(kiteq);
            }

        }


        LOGGER.info("ALL KITEQ SERVER|" + topic2Server + " ...");
        //创建所有可以使用的kiteqServer的连接
        for (final String server : serverList) {
            FutureTask<KiteIOClient> future = new FutureTask<KiteIOClient>(new Callable<KiteIOClient>() {
                @Override
                public KiteIOClient call() throws Exception {
                    return ClientManager.this.createKiteIOClient(server);
                }
            });
            future.run();
            this.hostport2Server.put(server, future);
            LOGGER.info("createKiteIOClient|" + server + "|SUCC ...");
        }

        //根据下来的kiteQclient创建连接
        for (Map.Entry<String, Set<String>> entry : topic2Server.entrySet()) {
            List<KiteIOClient> list = this.topic2Servers.get(entry.getKey());
            if (null == list) {
                list = new CopyOnWriteArrayList<KiteIOClient>();
                this.topic2Servers.put(entry.getKey(), list);
            }

            //将真正的连接放入
            for (String addr : entry.getValue()) {
                list.add(this.hostport2Server.get(addr).get(10, TimeUnit.SECONDS));
                //填写对应关系
                Set<String> topics = this.hostport2Topics.get(addr);
                if (null == topics) {
                    topics = new ConcurrentSkipListSet<String>();
                    this.hostport2Topics.put(addr, topics);
                }

                //添加topic
                topics.add(entry.getKey());
            }
        }

        //启动重连任务
        this.reconnectManager = new ReconnectManager();
        this.reconnectManager.setMaxReconTimes(maxReconTimes);
        this.reconnectManager.start();
        LOGGER.info("ClientManager|SUCC|" + this.topic2Servers + "...");
    }

    /**
     * only for test
     *
     * @param topic
     * @return
     * @throws NoKiteqServerException
     */
    List<KiteIOClient> getClient(String topic) throws NoKiteqServerException {
        List<KiteIOClient> serverUris = this.topic2Servers.get(topic);
        if (serverUris == null || serverUris.isEmpty()) {
            throw new NoKiteqServerException(topic);
        }
        return serverUris;
    }

    /**
     * 获取随机策略的client
     *
     * @param topic
     * @return
     * @throws NoKiteqServerException
     */
    public KiteIOClient findClient(String topic) throws NoKiteqServerException {
        List<KiteIOClient> serverUris = this.topic2Servers.get(topic);
        if (serverUris == null || serverUris.isEmpty()) {
            throw new NoKiteqServerException(topic);
        }

        KiteIOClient client = null;
        int i = 0;
        do {
            if (serverUris.isEmpty()) {
                throw new NoKiteqServerException(topic);
            }
            client = serverUris.get(RandomUtils.nextInt(0, serverUris.size()));
            //如果是dead,并且当前的hostport到topic的列表中存在该机器,则丢给重连任务
            if (client.isDead() && this.hostport2Topics.containsKey(client.getHostPort())) {
                this.reconnectManager.submitReconnect(client, this.callback);
                //移除掉
                serverUris.remove(client);
            } else {
                break;
            }

        } while ((i++) < 3);
        return client;
    }


    public void close() {
        for (Map.Entry<String, FutureTask<KiteIOClient>> entry : hostport2Server.entrySet()) {
            try {
                KiteIOClient c = entry.getValue().get();
                if (null != c) {
                    c.close();
                }
            } catch (Exception e) {
                LOGGER.error("ClientManager|CLOSE|KITE CLIENT|ERROR|" + entry.getKey(), e);
            }
        }
    }


    @Override
    protected void qServerNodeChange(String topic, List<String> address) {
        //获取到新的topic对应的kiteqServer
        LOGGER.info("ClientManager|qServerNodeChange|" + topic + "|" + address);
        if (!this.topics.contains(topic)) {
            return;
        }


        for (final String addr : address) {

            FutureTask<KiteIOClient> future = new FutureTask<KiteIOClient>(new Callable<KiteIOClient>() {
                @Override
                public KiteIOClient call() throws Exception {
                    //创建该连接
                    try {
                        return ClientManager.this.createKiteIOClient(addr);

                    } catch (Exception e) {
                        LOGGER.error("ClientManager|qServerNodeChange|FAIL" + addr, e);
                        throw e;
                    }
                }
            });
            FutureTask<KiteIOClient> exist = this.hostport2Server.putIfAbsent(addr, future);

            if (null == exist) {
                future.run();
            }
        }

        List<KiteIOClient> kiteIOClients = new CopyOnWriteArrayList<KiteIOClient>();
        for (String addr : address) {
            KiteIOClient client = null;
            try {
                client = this.hostport2Server.get(addr).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.error("ClientManager|qServerNodeChange|KITE CLIENT|ERROR|" + addr, e);
            }
            if (null == client) {
                LOGGER.warn("ClientManager|qServerNodeChange|NO KITE CLIENT|" + addr);
            } else {
                kiteIOClients.add(client);
            }

            //填写对应关系
            Set<String> topics = this.hostport2Topics.get(addr);
            //如果当前地址addr包含本topic则忽略
            if (null == topics) {
                //如果地址不存在topic
                topics = new ConcurrentSkipListSet<String>();
                this.hostport2Topics.put(addr, topics);
            }
            topics.add(topic);
        }

        //清理掉不包含在新地址中的对应关系
        for (Map.Entry<String, Set<String>> entry : this.hostport2Topics.entrySet()) {
            for (String t : entry.getValue()) {
                if (t.equalsIgnoreCase(topic) && !address.contains(entry.getKey())) {
                    //从地址中清理掉旧的对应关系
                    entry.getValue().remove(topic);
                    LOGGER.info("ClientManager|qServerNodeChange|Remove Topic IOClients|" + topic + "|" + entry.getKey());
                    break;
                }
            }
        }
        Iterator<String> it = this.hostport2Topics.keySet().iterator();
        for (; it.hasNext(); ) {
            String next = it.next();
            if (this.hostport2Topics.get(next).isEmpty()) {
                this.hostport2Topics.remove(next);
                this.hostport2Server.remove(next);
                LOGGER.info("ClientManager|qServerNodeChange|Remove IOClients|" + topic + "|" + next);
            }
        }

        //topic对应的地址列表更新
        this.topic2Servers.replace(topic, kiteIOClients);
        LOGGER.info("ClientManager|qServerNodeChange|ReInit IOClients|SUCC|" + topic + "|" + address + "|" +
                this.hostport2Topics);
    }

    /**
     * 创建物理连接
     *
     * @param hostport
     * @return
     * @throws Exception
     */
    private KiteIOClient createKiteIOClient(String hostport) throws Exception {
        return this.createKiteIOClient(hostport, clientConfigs.groupId, clientConfigs.secretKey, this.listener);
    }

    /**
     * 创建物理连接
     *
     * @param hostport
     * @return
     * @throws Exception
     */
    protected KiteIOClient createKiteIOClient(String hostport, String groupId,
                                              String secretKey, RemotingListener listener) throws Exception {
        final KiteIOClient kiteIOClient =
                new NettyKiteIOClient(groupId, secretKey, hostport, listener);
        kiteIOClient.start();
        return kiteIOClient;
    }

}

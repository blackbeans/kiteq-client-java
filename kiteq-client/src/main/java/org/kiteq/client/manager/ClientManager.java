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

/**
 * luofucong at 2015-03-05.
 */
public class ClientManager extends AbstractChangeWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);


    private final ConcurrentMap<String, List<String>> topic2Servers = new ConcurrentHashMap<String, List<String>>();

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
        public void callback(boolean succ, final KiteIOClient client) {
            try {
                synchronized (ClientManager.this.lock) {
                    //重连成功并且host到server的地址映射存在则需要重建
                    if (succ && ClientManager.this.hostport2Topics.containsKey(client.getHostPort())) {
                        //如果成功需要恢复所有topic中该client
                        ConcurrentMap<String/*hostport*/, Set<String>> tmp = ClientManager.this.hostport2Topics;

                        Set<String> topics = tmp.get(client.getHostPort());
                        //如果hostport没有对应的topic列表则认为无效的连接直接关闭
                        if (null == topics || topics.isEmpty()) {
                            ClientManager.this.hostport2Topics.remove(client.getHostPort());
                            //删除该hostport
                            ClientManager.this.hostport2Server.remove(client.getHostPort());
                            //关闭连接
                            client.close();
                            return;
                        }

                        for (String topic : topics) {
                            List<String> clients = ClientManager.this.topic2Servers.get(topic);
                            if (null == clients) {
                                clients = new CopyOnWriteArrayList<String>();
                                List<String> exist = ClientManager.this.topic2Servers.putIfAbsent(topic, clients);
                                if (null != exist) {
                                    clients = exist;
                                }
                            }

                            boolean needAdd = true;
                            Iterator<String> it = clients.iterator();
                            while (it.hasNext()) {
                                String kc = it.next();
                                //如果已经存在则放弃
                                if (kc.equalsIgnoreCase(client.getHostPort())) {
                                    needAdd = false;
                                    break;

                                }
                            }
                            //不存在先添加
                            if (needAdd) {
                                //添加该client
                                clients.add(client.getHostPort());
                            }
                        }

                        //强制覆盖掉
                        FutureTask<KiteIOClient> task = new FutureTask<KiteIOClient>(new Callable<KiteIOClient>() {
                            @Override
                            public KiteIOClient call() throws Exception {
                                return client;
                            }
                        });
                        task.run();

                        //将hostport放到对应关系里
                        FutureTask<KiteIOClient> exist = ClientManager.this.hostport2Server.put(client.getHostPort(), task);
                        if(null != exist){
                            try {
                              KiteIOClient existClient = exist.get(10 ,TimeUnit.SECONDS);
                              if(null != existClient && !existClient.isDead()){
                                  //关闭连接
                                  existClient.close();
                                  LOGGER.info("ClientManager|ReconnectManager|Callback|REPLACE CLIENT|CLOSE EXIST |"+client.getHostPort());
                              }
                            } catch (Exception e) {
                                LOGGER.error("ClientManager|ReconnectManager|Callback|REPLACE CLIENT|CLOSE EXIST |"+client.getHostPort(),e);
                            }
                        }


                    } else {

                        try {
                            Set<String> topics = ClientManager.this.hostport2Topics.remove(client.getHostPort());
                            ClientManager.this.hostport2Server.remove(client.getHostPort());
                            if (null != topics) {
                                //直接删除对应这个连接下的的所有topic的对应关系
                                for (String topic : topics) {
                                    List<String> clients = ClientManager.this.topic2Servers.get(topic);
                                    if (null != clients) {
                                        clients.remove(client.getHostPort());
                                    }
                                }
                            }
                        } finally {
                            //总要关闭的
                            client.close();
                            LOGGER.info("ClientManager|ReconnectManager|Callback|EXPIRED CLIENT |"+client.getHostPort());
                        }

                    }
                }
            } finally {
                LOGGER.info("ClientManager|ReconnectManager|Callback|SUCC|\nhostport2Topics:" +
                        ClientManager.this.hostport2Topics + "|\nhostport2Server:" +
                        ClientManager.this.hostport2Server.keySet() + "|\ntopic2Servers:" +
                        ClientManager.this.topic2Servers);
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
            List<String> list = this.topic2Servers.get(entry.getKey());
            if (null == list) {
                list = new CopyOnWriteArrayList<String>();
                this.topic2Servers.put(entry.getKey(), list);
            }

            //将真正的连接放入
            for (String addr : entry.getValue()) {
                list.add(addr);
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

        //开启连接存活检查
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                synchronized (ClientManager.this.lock) {
                    ClientManager.this.checkIOClient();
                }
            }
        }, 10, 30, TimeUnit.SECONDS);
        LOGGER.info("ClientManager|SUCC|" + this.topic2Servers + "...");
    }


    /**
     * 检查当前IOClient是否存在断开连接的情况
     */
    private void checkIOClient() {
        for (Map.Entry<String, FutureTask<KiteIOClient>> entry : this.hostport2Server.entrySet()) {
            KiteIOClient client = null;
            try {
                client = entry.getValue().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.error("ClientManager|checkIOClient|KITE CLIENT|ERROR|" + entry.getKey(), e);
            }
            if (null == client) {
                LOGGER.warn("ClientManager|checkIOClient|NO KITE CLIENT|" + entry.getKey());
            } else {
                //如果检测到已经不存活则需要重连
                if (client.isDead()) {
                    this.reconnectManager.submitReconnect(client, this.callback);
                    for (Map.Entry<String, List<String>> ie : this.topic2Servers.entrySet()) {
                        ie.getValue().remove(client.getHostPort());
                    }
                }
            }
        }
        LOGGER.info("ClientManager|checkIOClient|SUCC...");
    }

    /**
     * only for test
     *
     * @param topic
     * @return
     * @throws NoKiteqServerException
     */
    List<KiteIOClient> getClient(String topic) throws NoKiteqServerException {
        List<String> serverUris = this.topic2Servers.get(topic);
        if (serverUris == null || serverUris.isEmpty()) {
            throw new NoKiteqServerException(topic);
        }

        List<KiteIOClient> clients = new ArrayList<KiteIOClient>();
        for (String url : serverUris) {
            KiteIOClient client = null;
            try {
                client = this.hostport2Server.get(url).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.error("ClientManager|findClient|FAIL|" + url, e);
            } finally {
                if (null != client) {
                    clients.add(client);
                }
            }
        }
        return clients;
    }

    /**
     * 获取随机策略的client
     *
     * @param topic
     * @return
     * @throws NoKiteqServerException
     */
    public KiteIOClient findClient(String topic) throws NoKiteqServerException {
        List<String> serverUris = this.topic2Servers.get(topic);
        if (serverUris == null || serverUris.isEmpty()) {
            throw new NoKiteqServerException(topic);
        }

        KiteIOClient client = null;
        int i = 0;
        do {
            if (serverUris.isEmpty()) {
                throw new NoKiteqServerException(topic);
            }
            String hp = serverUris.get(RandomUtils.nextInt(0, serverUris.size()));
            KiteIOClient tmp = null;
            try {
                tmp = this.hostport2Server.get(hp).get(10, TimeUnit.SECONDS);
                //如果是dead,则丢给重连任务并清理掉
                if (tmp.isDead()) {
                    this.reconnectManager.submitReconnect(tmp, this.callback);
                    //从topic到hostport的列表中移除掉
                    serverUris.remove(hp);
                } else if (!tmp.isDead()) {
                    client = tmp;
                    break;
                }
            } catch (Exception e) {
                //DO NOTHING
                LOGGER.error("ClientManager|findClient|FAIL|" + hp, e);
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


        for (String addr : address) {
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

        //topic对应的地址列表更新
        this.topic2Servers.put(topic, new CopyOnWriteArrayList<String>(address));

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
            String hp = it.next();
            if (this.hostport2Topics.get(hp).isEmpty()) {
                it.remove();
                FutureTask<KiteIOClient> futureTask = this.hostport2Server.remove(hp);
                if (null != futureTask) {
                    try {
                        KiteIOClient client = futureTask.get(10, TimeUnit.SECONDS);
                        if (null != client) {
                            client.close();
                        }
                    } catch (Exception e) {
                        LOGGER.error("ClientManager|qServerNodeChange|REMOVE EMPTY HOSTPORT|FAIL|" + hp, e);
                    }
                }
                LOGGER.error("ClientManager|qServerNodeChange|REMOVE EMPTY HOSTPORT|" + hp);
            }
        }


        LOGGER.info("ClientManager|qServerNodeChange|ReInit IOClients|SUCC|" + topic + "|" + address + "|" +
                this.hostport2Topics + "|" + this.hostport2Server + "|" + this.topic2Servers);
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

package org.kiteq.client;

import org.apache.commons.lang3.RandomUtils;
import org.kiteq.client.binding.AbstractChangeWatcher;
import org.kiteq.client.binding.QServerManager;
import org.kiteq.client.message.MessageListener;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * luofucong at 2015-03-05.
 */
public class ClientManager extends AbstractChangeWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);


    private final ConcurrentMap<String, List<KiteIOClient>> topic2Servers = new ConcurrentHashMap<String, List<KiteIOClient>>();

    private final ConcurrentMap<String/*hostport*/,FutureTask<KiteIOClient>> hostport2Server
            = new ConcurrentHashMap<String, FutureTask<KiteIOClient>>();

    //当前支持的所有的topic列表
    private Set<String> topics = Collections.emptySet();

    private final QServerManager qserverManager;

    private final ClientConfigs clientConfigs;

    private final QRemotingListener listener;

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public ClientManager(QServerManager qServerManager, ClientConfigs clientConfigs, MessageListener listener) {
        this.qserverManager = qServerManager;
        this.clientConfigs = clientConfigs;
        this.listener = new QRemotingListener(listener);
    }


    public void init() throws Exception {
        super.zkClient = this.qserverManager.getZkClient();
        //设置所有的topic列表
        Set<String> serverList = new HashSet<String>();
        Map<String, List<String>> topic2Server = new HashMap<String, List<String>>();
        for (String topic : topics) {
            //获取对应的kiteq的server
            List<String> kiteq = this.qserverManager.pullAndWatchQServer(topic, this);
            if (null != kiteq && !kiteq.isEmpty()) {
                topic2Server.put(topic, kiteq);
                serverList.addAll(kiteq);
            }
        }

        LOGGER.info("ALL KITEQ SERVER|" + topic2Servers + " ...");
        //创建所有可以使用的kiteqServer的连接
        for (final String server : serverList) {
            FutureTask<KiteIOClient> future = new FutureTask<KiteIOClient>(new Callable<KiteIOClient>() {
                @Override
                public KiteIOClient call() throws Exception {
                   return ClientManager.this.createKiteIOClient(server);
                }
            });
            future.run();
            KiteIOClient client = this.createKiteIOClient(server);
            this.hostport2Server.put(server, future);
            LOGGER.info("createKiteIOClient|" + server + "|SUCC ...");
        }

        //根据下来的kiteQclient创建连接
        for (Map.Entry<String, List<String>> entry : topic2Server.entrySet()) {
            List<KiteIOClient> list = this.topic2Servers.get(entry.getKey());
            if (null == list) {
                list = new CopyOnWriteArrayList<KiteIOClient>();
                this.topic2Servers.put(entry.getKey(), list);
            }

            //将真正的连接放入
            for (String addr : entry.getValue()) {
                list.add(this.hostport2Server.get(addr).get(10,TimeUnit.SECONDS));
            }
        }

        LOGGER.info("ClientManager|SUCC...");
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
        KiteIOClient client = serverUris.get(RandomUtils.nextInt(0, serverUris.size()));
        return client;
    }


    public void close() {
        for (Map.Entry<String,FutureTask< KiteIOClient>> entry : hostport2Server.entrySet()) {
            try {
                KiteIOClient c = entry.getValue().get();
                if (null != c) {
                    c.close();
                }
            } catch (Exception e){
                LOGGER.error("qServerNodeChange|CLOSE|KITE CLIENT|ERROR|" +entry.getKey() ,e);
            }
        }
    }


    @Override
    protected void qServerNodeChange(String topic, List<String> address) {
        //获取到新的topic对应的kiteqServer
        LOGGER.info("qServerNodeChange|" + topic + "|" + address);
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
                        LOGGER.error("qServerNodeChange|createKiteIOClient|FAIL" + addr, e);
                        throw e;
                    }
                }
            });
            FutureTask<KiteIOClient> exist = this.hostport2Server.putIfAbsent(addr,future);

            if (null == exist){
                future.run();
            }
        }

        List<KiteIOClient> kiteIOClients = new CopyOnWriteArrayList<KiteIOClient>();
        for (String addr : address) {
            KiteIOClient client = null;
            try {
                client = this.hostport2Server.get(addr).get(10 ,TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.error("qServerNodeChange|KITE CLIENT|ERROR|" + addr,e);
            }
            if (null == client) {
                LOGGER.warn("qServerNodeChange|NO KITE CLIENT|" + addr);
            } else {
                kiteIOClients.add(client);
            }
        }

        //topic对应的地址列表更新
        this.topic2Servers.replace(topic, kiteIOClients);
        LOGGER.info("qServerNodeChange|ReInit IOClients|SUCC|" + topic + "|" + address);
    }


    /**
     * 创建物理连接
     * @param hostport
     * @return
     * @throws Exception
     */
    private KiteIOClient createKiteIOClient(String hostport) throws Exception {
        final KiteIOClient kiteIOClient =
                new NettyKiteIOClient(clientConfigs.groupId, clientConfigs.secretKey, hostport,this.listener);
        kiteIOClient.start();
        return kiteIOClient;
    }

}

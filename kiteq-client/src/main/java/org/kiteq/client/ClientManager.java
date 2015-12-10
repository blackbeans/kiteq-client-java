package org.kiteq.client;

import com.google.common.collect.MapMaker;
import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.kiteq.client.binding.BindingManager;
import org.kiteq.client.message.Message;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.TxResponse;
import org.kiteq.client.util.AckUtils;
import org.kiteq.client.util.MessageUtils;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.commons.threadpool.ThreadPoolManager;
import org.kiteq.commons.util.NamedThreadFactory;
import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.Protocol;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.kiteq.remoting.listener.KiteListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * luofucong at 2015-03-05.
 */
public class ClientManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientManager.class);

    private static final Logger DEBUGGER_LOGGER = LoggerFactory.getLogger("debugger");

    private final ConcurrentMap<String, KiteIOClient> waitingClients = new ConcurrentHashMap<String, KiteIOClient>();

    private final ConcurrentMap<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();

    private final ConcurrentMap<KiteIOClient, Boolean> clients = new MapMaker().weakKeys().makeMap();

    private final BindingManager bindingManager;

    private final ClientConfigs clientConfigs;

    private final MessageListener listener;

    public ClientManager(BindingManager bindingManager, ClientConfigs clientConfigs, MessageListener listener) {
        this.bindingManager = bindingManager;
        this.clientConfigs = clientConfigs;
        this.listener = listener;

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("IOClientsAliveChecker"));
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Iterator<KiteIOClient> iterator = clients.keySet().iterator();
                while (iterator.hasNext()) {
                    final KiteIOClient client = iterator.next();
                    if (client.isDead()) {
                        final String serverUri = client.getServerUrl();
                        final KiteIOClient ioClient = connMap.remove(serverUri);
                        if (ioClient != null) {
                            if (DEBUGGER_LOGGER.isDebugEnabled()) {
                                DEBUGGER_LOGGER.debug(ioClient + "start to reconnect due to heartbeat stopping.");
                            }

                            if (waitingClients.putIfAbsent(serverUri, ioClient) == null) {
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        if (ioClient.reconnect()) {
                                            waitingClients.remove(serverUri);

                                            putIfAbsent(serverUri, ioClient);
                                        }
                                    }
                                }, "Reconnecting(" + serverUri + ")Task").start();
                            }
                        }

                        iterator.remove();
                    }
                }
            }
        }, 1, 2, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                executor.shutdownNow();
            }
        }));
    }

    public KiteIOClient get(String topic) throws NoKiteqServerException {
        List<String> serverUris = bindingManager.getServerList(topic);
        if (serverUris == null || serverUris.isEmpty()) {
            throw new NoKiteqServerException(topic);
        }
        String serverUri = serverUris.get(RandomUtils.nextInt(0, serverUris.size()));
        return connMap.get(serverUri);
    }

    private KiteIOClient putIfAbsent(String serverUrl, KiteIOClient client) {
        KiteIOClient _client = connMap.putIfAbsent(serverUrl, client);
        if (_client == null) {
            clients.put(client, true);
        }
        return _client;
    }

    private KiteIOClient remove(String serverUrl) {
        KiteIOClient client = connMap.remove(serverUrl);
        if (client != null) {
            clients.remove(client);
        }
        return client;
    }

    public void close() {
        for (KiteIOClient client : connMap.values()) {
            client.close();
        }
    }

    public void refreshServers(String topic, List<String> newServerUris) {
        for (String newServerUri : newServerUris) {
            if (!connMap.containsKey(newServerUri)) {
                createKiteQClient(topic, newServerUri);
            }
        }
    }

    private void createKiteQClient(final String topic, final String serverUri) {
        KiteIOClient kiteIOClient = connMap.get(serverUri);
        if (kiteIOClient == null) {
            try {
                kiteIOClient = createKiteIOClient(serverUri);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create kiteq IO client: server=" + serverUri, e);
            }

            if (kiteIOClient.handshake()) {
                KiteIOClient _kiteIOClient;
                if ((_kiteIOClient = putIfAbsent(serverUri, kiteIOClient)) != null) {
                    kiteIOClient = _kiteIOClient;
                }
            } else {
                throw new RuntimeException(
                        "Unable to create kiteq IO client: server=" + serverUri + ", handshake refused.");
            }
        }

        kiteIOClient.getAcceptedTopics().add(topic);

        CuratorFramework curator = bindingManager.getCurator();
        try {
            final KiteIOClient finalKiteIOClient = kiteIOClient;
            curator.checkExists().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                        if (DEBUGGER_LOGGER.isDebugEnabled()) {
                            DEBUGGER_LOGGER.debug("[ZkEvents] Received " + watchedEvent);

                            DEBUGGER_LOGGER.debug("Remove topic " + topic + " from " + finalKiteIOClient);
                        }

                        finalKiteIOClient.getAcceptedTopics().remove(topic);

                        if (finalKiteIOClient.getAcceptedTopics().isEmpty()) {
                            remove(serverUri);

                            finalKiteIOClient.close();
                            LOGGER.warn(finalKiteIOClient + " closed.");
                        }
                    }
                }
            }).forPath(BindingManager.SERVER_PATH + topic + "/" + serverUri);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private KiteIOClient createKiteIOClient(String serverUri) throws Exception {
        final KiteIOClient kiteIOClient = new NettyKiteIOClient(clientConfigs.groupId, clientConfigs.secretKey, serverUri);
        kiteIOClient.start();

        kiteIOClient.registerListener(new KiteListener() {
            // handle transaction response
            @Override
            public void txAckReceived(final KiteRemoting.TxACKPacket txAck) {
                final TxResponse txResponse = TxResponse.parseFrom(txAck);

                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onMessageCheck(txResponse);
                        KiteRemoting.TxACKPacket txAckSend = txAck.toBuilder()
                                .setStatus(txResponse.getStatus()).build();
                        kiteIOClient.send(Protocol.CMD_TX_ACK, txAckSend);
                    }
                });
            }

            private void innerReceived(Message message) {
                boolean succ = false;
                try {
                    succ =listener.onMessage(message);
                } catch (Exception e) {
                    DEBUGGER_LOGGER.error("bytesMessageReceived|FAIL|",e);
                    succ = false;
                }
                KiteRemoting.DeliverAck ack = AckUtils.buildDeliverAck(message.getHeader(),succ);
                kiteIOClient.send(Protocol.CMD_DELIVER_ACK, ack);
            }
            // handle bytes message
            @Override
            public void bytesMessageReceived(final KiteRemoting.BytesMessage message) {
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        innerReceived(MessageUtils.convertMessage(message));
                    }

                });
            }

            // handle string message
            @Override
            public void stringMessageReceived(final KiteRemoting.StringMessage message) {
                ThreadPoolManager.getWorkerExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        innerReceived(MessageUtils.convertMessage(message));
                    }
                });
            }
        });
        return kiteIOClient;
    }
}

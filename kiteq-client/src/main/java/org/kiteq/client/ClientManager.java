package org.kiteq.client;

import com.google.common.collect.MapMaker;
import org.kiteq.commons.util.NamedThreadFactory;
import org.kiteq.remoting.client.KiteIOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.*;

/**
 * luofucong at 2015-03-05.
 */
public class ClientManager {

    private static final Logger DEBUGGER_LOGGER = LoggerFactory.getLogger("debugger");

    private final ConcurrentMap<String, KiteIOClient> connMap = new ConcurrentHashMap<String, KiteIOClient>();

    private final ConcurrentMap<KiteIOClient, Boolean> clients = new MapMaker().weakKeys().makeMap();

    public ClientManager() {
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("IOClientsAliveChecker"));
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Iterator<KiteIOClient> iterator = clients.keySet().iterator();
                while (iterator.hasNext()) {
                    KiteIOClient client = iterator.next();
                    if (client.isDead()) {
                        KiteIOClient ioClient = connMap.remove(client.getServerUrl());
                        if (ioClient != null) {
                            ioClient.close();

                            if (DEBUGGER_LOGGER.isDebugEnabled()) {
                                DEBUGGER_LOGGER.debug("Close " + ioClient + " due to heartbeat stopping.");
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

    public KiteIOClient get(String serverUrl) {
        return connMap.get(serverUrl);
    }

    public KiteIOClient putIfAbsent(String serverUrl, KiteIOClient client) {
        KiteIOClient _client = connMap.putIfAbsent(serverUrl, client);
        if (_client == null) {
            clients.put(client, true);
        }
        return _client;
    }

    public KiteIOClient remove(String serverUrl) {
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
}

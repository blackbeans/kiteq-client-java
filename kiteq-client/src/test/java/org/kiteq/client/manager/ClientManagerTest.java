package org.kiteq.client.manager;

import junit.framework.TestCase;
import org.kiteq.client.binding.AbstractChangeWatcher;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.binding.QServerManager;
import org.kiteq.client.manager.ClientConfigs;
import org.kiteq.client.manager.ClientManager;
import org.kiteq.client.message.Message;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.TxResponse;
import org.kiteq.remoting.client.KiteIOClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by blackbeans on 12/16/15.
 */
public class ClientManagerTest extends TestCase {

    MockClientManager manager;
    QServerManager qServerManager;

    @Override
    public void setUp() throws Exception {


        qServerManager = new QServerManager();
        qServerManager.setZkAddr("localhost:2181");
        qServerManager.init();

        ClientConfigs clientConfigs = new ClientConfigs();
        clientConfigs.setGroupId("s-mts-group");
        clientConfigs.setSecretKey("helloworld");
        manager = new MockClientManager(qServerManager, clientConfigs, new MessageListener() {
            @Override
            public boolean onMessage(Message message) {
                return true;
            }

            @Override
            public void onMessageCheck(TxResponse tx) {
                tx.commit();
            }
        });


    }

    public void testClientManager() throws Exception {
        if (null != qServerManager.getZkClient().checkExists().forPath(("/kiteq"))) {
            qServerManager.getZkClient().delete().deletingChildrenIfNeeded().forPath("/kiteq");
        }

        qServerManager.getZkClient().create().creatingParentsIfNeeded().forPath(QServerManager.PATH_SERVER + "/trade/127.0.0.1:13800");
        qServerManager.getZkClient().create().forPath(QServerManager.PATH_SERVER + "/trade/127.0.0.2:13800");

        Set<String> topics = new HashSet<String>();
        topics.add("trade");
        manager.setTopics(topics);
        manager.init();

        KiteIOClient client = manager.findClient("trade");
        System.out.println("findClient=====" + client);
        assertNotNull(client);
        assertTrue(client.getHostPort().equalsIgnoreCase("127.0.0.1:13800") ||
                client.getHostPort().equalsIgnoreCase("127.0.0.2:13800"));


        //测试一下重连,设置127.0.0.1:13800为断开需要重连,并开始触发重连操作
        this.manager.isDead = true;
        client = manager.findClient("trade");
        System.out.println("findClient=====" + client);
        assertNotNull(client);
        assertTrue(client.getHostPort().equalsIgnoreCase("127.0.0.2:13800"));

        //等待10s后触发重连
        Thread.sleep(20 * 1000);
        //按理已经发起了2次重连,所以设置为true
        this.manager.isDead = false;


        List<KiteIOClient> clients = manager.getClient("trade");
        System.out.println("findClient=====" + client);
        assertEquals(clients.size(), 2);
        boolean succ = false;
        for (KiteIOClient c : clients) {
            c.getHostPort().equalsIgnoreCase("127.0.0.1:13800");
            succ = true;
            break;
        }

        assertTrue(succ);


        qServerManager.getZkClient().delete().forPath(QServerManager.PATH_SERVER + "/trade/127.0.0.2:13800");
        Thread.sleep(10 * 1000);
        client = manager.findClient("trade");
        System.out.println("findClient=====" + client);
        assertNotNull(client);
        assertTrue(client.getHostPort().equalsIgnoreCase("127.0.0.1:13800"));


    }

    @Override
    public void tearDown() throws Exception {
        if (null != qServerManager.getZkClient().checkExists().forPath(("/kiteq"))) {
            qServerManager.getZkClient().delete().deletingChildrenIfNeeded().forPath("/kiteq");
        }
        this.manager.close();
    }
}

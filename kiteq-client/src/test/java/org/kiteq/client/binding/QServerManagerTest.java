package org.kiteq.client.binding;

import junit.framework.TestCase;
import org.kiteq.client.binding.AbstractChangeWatcher;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.binding.QServerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by blackbeans on 12/16/15.
 */
public class QServerManagerTest extends TestCase {

    QServerManager qServerManager;

    @Override
    public void setUp() throws Exception {

        qServerManager = new QServerManager();
        qServerManager.setZkAddr("localhost:2181");
        qServerManager.init();

        if(null != qServerManager.getZkClient().checkExists().forPath(("/kiteq"))){
            qServerManager.getZkClient().delete().deletingChildrenIfNeeded().forPath("/kiteq");
        }


    }

    public void testQServerChange() throws Exception {

        qServerManager.getZkClient().create().creatingParentsIfNeeded().forPath(QServerManager.PATH_SERVER + "/trade/127.0.0.1:13800");

        List<String> list = new ArrayList<String>();
        list.add("trade");
        qServerManager.publishTopics("s-mts-group", "localhost", list);
        List<Binding> binds = new ArrayList<Binding>();
        binds.add(Binding.bindDirect("s-mts-group", "trade", "pay-succ", -1, true));
        qServerManager.subscribeTopics("s-mts-group", binds);


        final CountDownLatch latch = new CountDownLatch(1);
        AbstractChangeWatcher watcher = new AbstractChangeWatcher() {

            @Override
            protected void qServerNodeChange(String topic, List<String> address) {
               if(latch.getCount()>0) {
                   assertTrue(address.size() == 2);
                   assertEquals(address.get(0), "127.0.0.1:13800");
                   assertEquals(address.get(1), "127.0.0.2:13800");
                   latch.countDown();
                   System.out.println(topic + "|" + address);
               }
            }
        };
        watcher.setZkClient(qServerManager.getZkClient());

        List<String> servers = qServerManager.pullAndWatchQServer("trade", watcher);


        assertTrue(!servers.isEmpty() && servers.contains("127.0.0.1:13800"));


        qServerManager.getZkClient().create().forPath(QServerManager.PATH_SERVER + "/trade/127.0.0.2:13800");

        boolean succ = latch.await(10, TimeUnit.SECONDS);
        assertTrue(succ);


    }

    @Override
    public void tearDown() throws Exception {
        if(null != qServerManager.getZkClient().checkExists().forPath(("/kiteq"))){
            qServerManager.getZkClient().delete().deletingChildrenIfNeeded().forPath("/kiteq");
        }
        qServerManager.destroy();
    }
}

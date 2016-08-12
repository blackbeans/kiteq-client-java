package org.kiteq.client.manager;

import org.kiteq.client.binding.QServerManager;
import org.kiteq.client.message.MessageListener;
import org.kiteq.commons.monitor.KiteQMonitor;
import org.kiteq.remoting.client.KiteIOClient;
import org.kiteq.remoting.client.impl.NettyKiteIOClient;
import org.kiteq.remoting.listener.RemotingListener;

/**
 * mock的ClientManager
 * Created by blackbeans on 12/16/15.
 */
public class MockClientManager extends ClientManager {
    public MockClientManager(QServerManager qServerManager, ClientConfigs clientConfigs, MessageListener listener) {
        super(qServerManager, clientConfigs, listener, (KiteQMonitor)null);
    }


     boolean isDead = false;

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
                new NettyKiteIOClient(groupId, secretKey, 10,hostport, listener){
                    @Override
                    public boolean isDead() {
                        return MockClientManager.this.isDead(this.getHostPort());
                    }

                    @Override
                    public boolean reconnect() {
                        super.reconnect();
                       return MockClientManager.this.reconnect(this.getHostPort(),this.getReconnectCount());
                    }


                };

        return kiteIOClient;
    }


    public boolean isDead(String hostport) {
        if(hostport.equalsIgnoreCase("127.0.0.1:13800")){
            return this.isDead;
        }
        return false;
    }


    public boolean reconnect(String hostport,int retryCount) {
        if(hostport.equalsIgnoreCase("127.0.0.1:13800") && retryCount>=2){
            return true;
        }
        return false;
    }
}

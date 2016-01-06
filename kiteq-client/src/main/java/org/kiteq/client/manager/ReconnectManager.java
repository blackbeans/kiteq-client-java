package org.kiteq.client.manager;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.log4j.Logger;
import org.kiteq.commons.util.NamedThreadFactory;
import org.kiteq.remoting.client.KiteIOClient;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 重连管理器
 * Created by blackbeans on 12/15/15.
 */
public class ReconnectManager {

    private static final Logger LOGGER = Logger.getLogger(ReconnectManager.class);

    //需要重连的连接
    private ConcurrentMap<String, KiteIOClient> reconnectors = new ConcurrentHashMap<String, KiteIOClient>();
    private final HashedWheelTimer timer =
            new HashedWheelTimer(new NamedThreadFactory("reconnector-", true), 1, TimeUnit.SECONDS, 10);

    private int maxReconTimes = 10;

    public void setMaxReconTimes(int maxReconTimes) {
        this.maxReconTimes = maxReconTimes;
    }


    /**
     * 开启重连任务
     */
    public void start() {
        this.timer.start();

    }

    public void stop(){
        this.timer.stop();
    }

    static interface IReconnectCallback{
         void callback(boolean succ,KiteIOClient client);
    }

    public void submitReconnect(final KiteIOClient kiteIOClient,final IReconnectCallback callback) {
        //只保留一个
        KiteIOClient exist = this.reconnectors.putIfAbsent(kiteIOClient.getHostPort(), kiteIOClient);
        if (null == exist) {
            LOGGER.info("ReconnectManager|submitReconnect|SUCC|" + kiteIOClient.getHostPort());
            this.timer.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {

                    LOGGER.warn("ReconnectManager|Reconnecting|"+kiteIOClient.getHostPort()+"|"+kiteIOClient.getReconnectCount());
                    if(!kiteIOClient.isDead()){
                        ReconnectManager.this.reconnectors.remove(kiteIOClient.getHostPort());
                        return;
                    }
                    //开启重连
                    boolean succ = kiteIOClient.reconnect();
                    LOGGER.warn("ReconnectManager|Reconnecting|SUCC:"+succ+"|"+kiteIOClient.getHostPort()+"|"+kiteIOClient.getReconnectCount());
                    if (succ) {
                        //如果成功则
                        callback.callback(succ,kiteIOClient);
                        ReconnectManager.this.reconnectors.remove(kiteIOClient.getHostPort());
                        return;
                    }

                    if (kiteIOClient.getReconnectCount() < ReconnectManager.this.maxReconTimes) {
                        //小于20次则进行重连
                        ReconnectManager.this.timer.newTimeout(this,
                                (long) (Math.pow(2, kiteIOClient.getReconnectCount())), TimeUnit.SECONDS);
                        return ;
                    }
                    callback.callback(false,kiteIOClient);
                    ReconnectManager.this.reconnectors.remove(kiteIOClient.getHostPort());
                    LOGGER.warn("ReconnectManager|Reconnecting|FAIL|Give UP|"+kiteIOClient.getHostPort()+"|"+kiteIOClient.getReconnectCount());

                }
            }, (long) (Math.pow(2, kiteIOClient.getReconnectCount())) , TimeUnit.SECONDS);
        }

    }
}

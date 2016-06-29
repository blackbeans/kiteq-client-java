package org.kiteq.commons.monitor;


import org.kiteq.commons.threadpool.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Result;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by blackbeans on 6/21/16.
 */
public class KiteQMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KiteQMonitor.class);

    private final ConcurrentMap<String,Counter> counters = new ConcurrentHashMap<String, Counter>();

    private static final ScheduledThreadPoolExecutor SCHEDULED_THREAD_POOL_EXECUTOR = new ScheduledThreadPoolExecutor(1);

    private IMonitorUpload monitorUpload;

    private String groupId;

    private String hostport;

    public void setMonitorUpload(IMonitorUpload monitorUpload) {
        this.monitorUpload = monitorUpload;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setHostport(String hostport) {
        this.hostport = hostport;
    }

    public void init(){
        SCHEDULED_THREAD_POOL_EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info(KiteQMonitor.this.formatOutput());
                } catch (Exception e) {
                    LOGGER.error("monitorUpload.formatOutput|FAIL", e);
                }
            }
        },  10,1, TimeUnit.SECONDS);
    }

    public void addData(String key, long count,long cost) {
        Counter exist = this.counters.get(key);
        if (null == exist) {
            exist = new Counter();
            Counter tmp = this.counters.putIfAbsent(key, exist);
            if (null != tmp) {
                exist = tmp;
            }
        }
        exist.incr(count);
        exist.incrTime(cost);
    }

    private String formatOutput(){

        Set<String> titles = new TreeSet<String>(this.counters.keySet());
        StringBuilder sb = new StringBuilder();
        for(String t :titles){
            sb.append(t.toUpperCase()).append("\t");
            sb.append(t.toUpperCase()).append("_AVG_COST\t");
        }
        sb.append("WP-MaxPool").append("\t");
        sb.append("WP-PoolSize").append("\t");
        sb.append("WP-QueueLen").append("\t");

        sb.append("\n\t");

        Map<String,Object> monitorData = new HashMap<String, Object>();
        for(String t :titles){
            Counter c = this.counters.get(t);
            Counter.CounterResult changed = c.changed();
            sb.append(changed.changed)
                    .append("\t")
                    .append(changed.avgCostMilSeconds)
                    .append("\t");
            monitorData.put(t.toUpperCase(), changed.changed);
            //平均耗时
            monitorData.put(t.toUpperCase()+"_AVG_COST",changed.avgCostMilSeconds);
        }

        //threadpool
        int maxPoolSize = ThreadPoolManager.getWorkerExecutor().getMaximumPoolSize();
        int poolSize = ThreadPoolManager.getWorkerExecutor().getPoolSize();
        int queueLen = ThreadPoolManager.getWorkerExecutor().getQueue().size();

        monitorData.put("WP-MaxPool",maxPoolSize);
        monitorData.put("WP-PoolSize",poolSize);
        monitorData.put("WP-QueueLen",queueLen);
        sb.append("\n");

        //update
        if (null != this.monitorUpload){
            try {
                this.monitorUpload.sendMonitorData("KiteQ-"+this.groupId,this.hostport,monitorData,System.currentTimeMillis());
            } catch (Exception e) {
                //ignored
                LOGGER.error("monitorUpload.sendMonitorData|FAIL|"+monitorData,e);
            }
        }


        return sb.toString();

    }


}

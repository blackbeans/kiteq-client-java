package org.kiteq.commons.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by blackbeans on 6/21/16.
 */
public class KiteQMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KiteQMonitor.class);

    private final ConcurrentMap<String,Counter> counters = new ConcurrentHashMap<String, Counter>();

    private static final ScheduledThreadPoolExecutor SCHEDULED_THREAD_POOL_EXECUTOR = new ScheduledThreadPoolExecutor(1);

    public void init(){
        SCHEDULED_THREAD_POOL_EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
               LOGGER.info(KiteQMonitor.this.formatOutput());
            }
        },0,1, TimeUnit.SECONDS);
    }

    public void addData(String key, int count) {
        Counter exist = this.counters.get(key);
        if (null == exist) {
            exist = new Counter();
            Counter tmp = this.counters.putIfAbsent(key, exist);
            if (null != tmp) {
                exist = tmp;
            }
        }
        exist.incr(count);
    }

    private String formatOutput(){

        Set<String> titles = new TreeSet<String>(this.counters.keySet());
        StringBuilder sb = new StringBuilder();
        for(String t :titles){
            sb.append(t.toUpperCase()).append("\t");
        }
        sb.append("\n\t");

        for(String t :titles){
            Counter c = this.counters.get(t);
            sb.append(c.changed())
            .append("\t");

        }
        sb.append("\n");

        return sb.toString();

    }


}

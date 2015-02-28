package org.kiteq.commons.stats;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.kiteq.commons.util.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class MessageStats {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageStats.class);
    
    private static Counter readCounter = new Counter();
    private static Counter writeCounter = new Counter();
    
    private static ScheduledExecutorService scheduledExecutorService;
    private static AtomicBoolean inited = new AtomicBoolean(false);
    
    private static String appName = System.getProperty("kiteq.appName", "");
    
    public static void recordRead() {
        readCounter.inc();
    }
    
    public static void recordWrite() {
        writeCounter.inc();
    }
    
    public static void start() {
        
        if (inited.compareAndSet(false, true)) {
            scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                
                @Override
                public void run() {
                    
//                    logger.warn(appName + " Stats - read: {}, write: {}",
//                            readCounter.getCountChange(),
//                            writeCounter.getCountChange());
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
    }
    
    public static void close() {
        scheduledExecutorService.shutdownNow();
    }

}

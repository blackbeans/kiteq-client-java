package org.kiteq.commons.threadpool;

import org.kiteq.commons.Configs;
import org.kiteq.commons.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author gaofeihang
 * @since Mar 2, 2015
 */
public class ThreadPoolManager {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);

    private static final int corePoolSize = Configs.THREAD_CORE_POOL_SIZE;
    private static final int maximumPoolSize = Configs.THREAD_MAX_POOL_SIZE;
    private static final long keepAliveTime = Configs.THREAD_KEEPALIVE_TIME;

    private static RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();

    private static ThreadPoolExecutor dispatcherExecutor;
    private static ThreadPoolExecutor workerExecutor;

    static {
        dispatcherExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, 
                new SynchronousQueue<Runnable>(), new NamedThreadFactory("KiteDispatcherProcessor"), handler);
        
        workerExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, 
                new SynchronousQueue<Runnable>(), new NamedThreadFactory("KiteWorkerProcessor"), handler);
    }
    
    public static ThreadPoolExecutor getDispatcherExecutor() {
        return dispatcherExecutor;
    }
    
    public static ThreadPoolExecutor getWorkerExecutor() {
        return workerExecutor;
    }
    
    public static void shutdown() {
        shutdownExecutor(dispatcherExecutor);
        shutdownExecutor(workerExecutor);
    }
    
    private static void shutdownExecutor(ThreadPoolExecutor executor) {
        try {
            executor.shutdown();
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            executor.shutdownNow();

        } catch (InterruptedException e) {
            logger.error("Shutdown threadpool executor error! {}", executor, e);
        }
    }

}

package org.kiteq.remoting.response;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class ResponseFuture implements Future<KiteResponse> {
    
    private static final Logger logger = LoggerFactory.getLogger(ResponseFuture.class); 
    
    private static ConcurrentHashMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
    
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    
    private String requestId;
    private volatile KiteResponse response;
    
    public ResponseFuture(String requestId) {
        this.requestId = requestId;
        
        if (futureMap.putIfAbsent(requestId, this) != null) {
            logger.warn("requestId conflict: {}, thread: {}", requestId, Thread.currentThread().getName());
        }
    }
    
    public static void receiveResponse(KiteResponse response) {
        String requestId = response.getRequestId();
        ResponseFuture future = futureMap.remove(requestId);
        if (future != null) {
            future.setResponse(response);
        }
    }
    
    public void setResponse(KiteResponse response) {
        this.response = response;
        
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public KiteResponse get() throws InterruptedException, ExecutionException {
        
        try {
            lock.lock();
            condition.await();
            return response;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public KiteResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        
        try {
            lock.lock();
            condition.await(timeout, unit);
            return response;
        } finally {
            futureMap.remove(requestId);
            lock.unlock();
        }
    }

}

package org.kiteq.remoting.frame;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class ResponsFuture implements Future<KiteResponse> {
    
    private static Map<String, ResponsFuture> futureMap = new ConcurrentHashMap<String, ResponsFuture>();
    
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    
    private volatile KiteResponse response;
    
    public ResponsFuture(long requestId) {
        futureMap.put(String.valueOf(requestId), this);
    }
    
    public ResponsFuture(String requestId) {
        futureMap.put(requestId, this);
    }
    
    public static void receiveResponse(KiteResponse response) {
        String requestId = response.getRequestId();
        ResponsFuture future = futureMap.get(requestId);
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
        return null;
    }

}

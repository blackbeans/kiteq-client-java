package org.kiteq.remoting.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class ResponseFuture implements Future<KiteResponse> {

    private static final Logger logger = LoggerFactory.getLogger(ResponseFuture.class);

    private static ConcurrentHashMap<Integer, ResponseFuture> futureMap = new ConcurrentHashMap<Integer, ResponseFuture>();
    
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    private int requestId;
    private volatile KiteResponse response;

    public ResponseFuture(int requestId) {
        this.requestId = requestId;
        
        if (futureMap.putIfAbsent(requestId, this) != null) {
            logger.warn("requestId conflict: {}, thread: {}", requestId, Thread.currentThread().getName());
        }
    }
    
    public static void receiveResponse(KiteResponse response) {
        int requestId = response.getRequestId();
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
            futureMap.remove(requestId);
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

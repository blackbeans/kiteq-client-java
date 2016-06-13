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
        lock.lock();
        try {
            if (null == this.response){
                this.response = response;
            }
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
        this.lock.lock();
        try {
            if (null == response) {
                condition.await();
            }
        } finally {
            futureMap.remove(requestId);
            this.lock.unlock();
        }
        return response;
    }

    @Override
    public KiteResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        this.lock.lock();
        try {
            if (null == response) {
                condition.await(timeout,unit);
            }
        } finally {
            futureMap.remove(requestId);
            this.lock.unlock();
        }
        return response;

    }

}

package org.kiteq.commons.monitor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by blackbeans on 6/21/16.
 */
public class Counter {


    private long preCount = 0L;
    private AtomicLong counter  = new AtomicLong();

    public long incr(int count ){
       return  this.counter.addAndGet(count);
    }

    public long changed(){

        long now = counter.get();
        long change =now- this.preCount;
        this.preCount = now;
        return change;

    }
}

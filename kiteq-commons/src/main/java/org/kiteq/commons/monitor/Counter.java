package org.kiteq.commons.monitor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by blackbeans on 6/21/16.
 */
public class Counter {

    static class CounterResult{
        long changed;
        long avgCostMilSeconds;
    }

    private long preCount = 0L;
    private long preTime = 0L;
    private AtomicLong counter  = new AtomicLong();
    //时间
    private AtomicLong time = new AtomicLong();

    public long incr(long count ){
       return  this.counter.addAndGet(count);
    }

    public long incrTime(long milseconds){
        return this.time.addAndGet(milseconds);
    }

    public CounterResult changed(){

        long now = counter.get();
        long change =now- this.preCount;
        this.preCount = now;

        long times = this.time.get();
        long tchange =times - this.preTime;
        this.preTime = times;


        //计算平均值
        CounterResult result = new CounterResult();
        result.changed = change;
        result.avgCostMilSeconds = (tchange/change);

        return result;

    }

}

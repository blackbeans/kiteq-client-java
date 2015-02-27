package org.kiteq.commons.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class Counter {
    
    private AtomicLong count = new AtomicLong(0);
    private long prevCount;
    
    public Counter() {}
    
    public Counter(int count) {
        this.count.set(count);
    }

    public long inc() {
        return count.incrementAndGet();
    }
    
    public long inc(long i) {
        return count.addAndGet(i);
    }
    
    public long dec() {
        return count.decrementAndGet();
    }
    
    public long dec(long i) {
        return count.addAndGet(-i);
    }
    
    public void clear() {
        count.set(0);
        prevCount = 0;
    }
    
    public long getCount() {
        return count.get();
    }
    
    public long getCountChange() {
        long tempCount = count.get();
        long change = tempCount - prevCount;
        prevCount = tempCount;
        return change;
    }
    
}

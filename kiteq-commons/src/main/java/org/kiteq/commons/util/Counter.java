package org.kiteq.commons.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public class Counter {

    private final AtomicReference<AtomicLong> counter = new AtomicReference<AtomicLong>(new AtomicLong(0));

    public long inc() {
        return counter.get().incrementAndGet();
    }
    
    public long getCountChange() {
        boolean cas;
        AtomicLong count;
        do {
            count = counter.get();
            cas = counter.compareAndSet(count, new AtomicLong(0));
        } while (!cas);
        return count.get();
    }
}

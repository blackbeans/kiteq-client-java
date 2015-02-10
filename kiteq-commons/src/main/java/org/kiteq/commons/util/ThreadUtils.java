package org.kiteq.commons.util;

/**
 * @author gaofeihang
 * @since Feb 5, 2015
 */
public class ThreadUtils {
    
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

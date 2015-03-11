package org.kiteq.commons;

/**
 * luofucong at 2015-03-11.
 */
public class Configs {

    private static final String THREAD_CORE_POOL_SIZE_NAME = "thread.pool.size.core";

    private static final String THREAD_MAX_POOL_SIZE_NAME = "thread.pool.size.max";

    private static final String THREAD_KEEPALIVE_TIME_NAME = "thread.time.keepalive";

    public static final int THREAD_CORE_POOL_SIZE = Integer.parseInt(System.getProperty(THREAD_CORE_POOL_SIZE_NAME, "5"));

    public static final int THREAD_MAX_POOL_SIZE = Integer.parseInt(System.getProperty(THREAD_MAX_POOL_SIZE_NAME, "100"));

    public static final long THREAD_KEEPALIVE_TIME = Long.parseLong(System.getProperty(THREAD_KEEPALIVE_TIME_NAME, "300"));
}

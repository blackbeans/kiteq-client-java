package org.kiteq.remoting.client.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class NettyIOClientTest {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyIOClientTest.class);
    
    private static final String GROUP_ID = "s-mts-test";
    private static final String SERVER_URL = "localhost:13800";
    
    private NettyKiteIOClient kiteQIOClient;
    
    @Before
    public void init() {
        try {
            kiteQIOClient = new NettyKiteIOClient(GROUP_ID, SERVER_URL);
            kiteQIOClient.start();
        } catch (Exception e) {
            logger.error("client init error!", e);
        }
    }
    
    @Test
    public void testHandshake() {
        boolean success = kiteQIOClient.handshake();
        Assert.assertEquals(true, success);
    }

}

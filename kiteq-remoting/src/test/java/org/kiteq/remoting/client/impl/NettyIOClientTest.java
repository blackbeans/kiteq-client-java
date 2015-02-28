package org.kiteq.remoting.client.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kiteq.commons.util.ThreadUtils;
import org.kiteq.protocol.KiteRemoting.ConnAuthAck;
import org.kiteq.protocol.KiteRemoting.ConnMeta;
import org.kiteq.protocol.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class NettyIOClientTest {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyIOClientTest.class);
    
    private static final String GROUP_ID = "s-mts-test";
    private static final String SECRET_KEY = "s-mts-test";
    private static final String SERVER_URL = "localhost:13800";
    
    private NettyKiteIOClient kiteIOClient;
    
    @Before
    public void init() {
        try {
            kiteIOClient = new NettyKiteIOClient(SERVER_URL);
            kiteIOClient.start();
        } catch (Exception e) {
            logger.error("client init error!", e);
        }
    }
    
    @After
    public void close() {
        ThreadUtils.sleep(3000);
        kiteIOClient.close();
    }
    
    @Test
    public void testHandshake() throws Exception {
        
        ConnMeta connMeta = ConnMeta.newBuilder()
                .setGroupId(GROUP_ID)
                .setSecretKey(SECRET_KEY)
                .build();
                
        ConnAuthAck ack = kiteIOClient.sendAndGet(Protocol.CMD_CONN_META, connMeta.toByteArray());
        
        Assert.assertEquals(true, ack.getStatus());
    }

}

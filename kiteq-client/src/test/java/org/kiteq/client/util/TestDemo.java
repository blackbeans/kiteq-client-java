package org.kiteq.client.util;

import com.immomo.mcf.util.LogWrapper;
import junit.framework.TestCase;
import org.slf4j.LoggerFactory;

/**
 * Created by blackbeans on 12/24/15.
 */
public class TestDemo  extends TestCase{


    public void testLog() throws Exception {

        LogWrapper.getLogger("stdout");

        LogInitUtils.initLog("test");
        LoggerFactory.getLogger(TestDemo.class).info("----------------");

    }
}

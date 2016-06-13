package org.kiteq.remoting.response;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by blackbeans on 6/13/16.
 */
public class ResponseFutureTest extends TestCase{

    @Test
    public void testFutureSetAndGetSleep() throws Exception{

        System.out.println("SLEEP 2s AND SET");
        final ResponseFuture future = new ResponseFuture(1);

        new Thread(){
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                future.setResponse(new KiteResponse(1,"succ"));
            }
        }.start();

        KiteResponse resp = future.get(5 , TimeUnit.SECONDS);


        assertTrue(null != resp);
        assertEquals(resp.getModel(),"succ");
        System.out.println(resp);


    }

    @Test
    public void testFutureSetAndGetFirst() throws Exception{

        System.out.println("SLEEP 2s AND First");
        final ResponseFuture future = new ResponseFuture(1);

        future.setResponse(new KiteResponse(1, "succ"));
        KiteResponse resp = future.get(5, TimeUnit.SECONDS);


        assertTrue(null != resp);
        assertEquals(resp.getModel(),"succ");
        System.out.println(resp);


    }



}

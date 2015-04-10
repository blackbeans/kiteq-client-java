package org.kiteq.client.message;

/**
 * @author gaofeihang
 * @since Apr 10, 2015
 */
public interface TxCallback {
    
    void doTransaction(TxResponse tx) throws Exception;

}

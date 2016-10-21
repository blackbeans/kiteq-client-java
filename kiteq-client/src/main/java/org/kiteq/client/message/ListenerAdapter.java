package org.kiteq.client.message;

import org.kiteq.commons.exception.KiteQClientException;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public abstract class ListenerAdapter implements MessageListener {


    @Override
    public void onMessageCheck(TxResponse tx) throws KiteQClientException {
        tx.commit();
    }

}

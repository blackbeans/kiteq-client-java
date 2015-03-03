package org.kiteq.client.message;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public abstract class ListenerAdapter implements MessageListener {
    
    @Override
    public boolean onMessage(Message message) {
        return true;
    }
    
    @Override
    public void onMessageCheck(TxResponse response) {
        response.commit();
    }

}

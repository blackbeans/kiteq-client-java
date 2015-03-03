package org.kiteq.client.message;


/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public interface MessageListener {
    
    boolean onMessage(Message message);
    
    void onMessageCheck(TxResponse response);

}

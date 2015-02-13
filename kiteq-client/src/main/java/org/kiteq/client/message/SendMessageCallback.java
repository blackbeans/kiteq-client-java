package org.kiteq.client.message;


/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface SendMessageCallback {
    
    Object doInTransaction(MessageStatus status);

}

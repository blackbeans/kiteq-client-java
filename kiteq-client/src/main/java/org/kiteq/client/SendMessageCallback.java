package org.kiteq.client;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public interface SendMessageCallback {
    
    Object doInTransaction(MessageStatus status);

}

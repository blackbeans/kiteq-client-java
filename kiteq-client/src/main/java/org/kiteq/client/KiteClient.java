package org.kiteq.client;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public interface KiteClient extends KitePublisher, KiteSubscriber {
    
    void start();
    
    void close();

}

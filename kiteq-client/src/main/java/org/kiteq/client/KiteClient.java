package org.kiteq.client;

import org.kiteq.client.binding.Binding;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.commons.exception.NoKiteqServerException;
import org.kiteq.protocol.KiteRemoting.BytesMessage;
import org.kiteq.protocol.KiteRemoting.StringMessage;

/**
 * @author gaofeihang
 * @since Feb 25, 2015
 */
public interface KiteClient {

    interface TxCallback {
        void doTransaction(TxResponse txResponse) throws Exception;
    }
    
    void setPublishTopics(String[] topics);
    
    void setBindings(Binding[] bindings);
    
    SendResult sendStringMessage(StringMessage message) throws NoKiteqServerException;
    
    SendResult sendBytesMessage(BytesMessage message) throws NoKiteqServerException;

    SendResult sendTxMessage(StringMessage message, TxCallback txCallback) throws NoKiteqServerException;

    SendResult sendTxMessage(BytesMessage message, TxCallback txCallback) throws NoKiteqServerException;
    
    void start();
    
    void close();

}

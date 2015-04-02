package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting;
import org.kiteq.protocol.KiteRemoting.TxACKPacket;
import org.kiteq.protocol.Protocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public class TxResponse {

    private String messageId;
    private int status;
    private String feedback;

    private final Map<String, String> properties = new HashMap<String, String>();

    public TxResponse() {

    }

    public TxResponse(KiteRemoting.Header header) {
        List<KiteRemoting.Entry> propertiesList = header.getPropertiesList();
        if (propertiesList != null) {
            for (KiteRemoting.Entry entry : propertiesList) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public int getStatus() {
        return status;
    }
    
    public void setStatus(int status) {
        this.status = status;
    }
    
    public String getFeedback() {
        return feedback;
    }
    
    public void setFeedback(String feedback) {
        this.feedback = feedback;
    }
    
    public void commit() {
        this.status = Protocol.TX_COMMIT;
    }
    
    public void rollback() {
        this.status = Protocol.TX_ROLLBACK;
    }
    
    public static TxResponse parseFrom(TxACKPacket txAck) {
        TxResponse txResponse = new TxResponse();
        List<KiteRemoting.Entry> propertiesList = txAck.getHeader().getPropertiesList();
        if (propertiesList != null) {
            for (KiteRemoting.Entry entry : propertiesList) {
                txResponse.properties.put(entry.getKey(), entry.getValue());
            }
        }
        txResponse.setStatus(txAck.getStatus());
        txResponse.setFeedback(txAck.getFeedback());
        return txResponse;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}

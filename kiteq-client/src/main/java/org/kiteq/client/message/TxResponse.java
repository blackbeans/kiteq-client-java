package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting.TxACKPacket;
import org.kiteq.protocol.Protocol;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public class TxResponse {
    
    private String messageId;
    private int status;
    private String feedback;
    
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
        txResponse.setMessageId(txAck.getMessageId());
        txResponse.setStatus(txAck.getStatus());
        txResponse.setFeedback(txAck.getFeedback());
        return txResponse;
    }

}

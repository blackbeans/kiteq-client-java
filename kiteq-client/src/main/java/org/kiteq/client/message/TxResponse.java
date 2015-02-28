package org.kiteq.client.message;

import org.kiteq.protocol.KiteRemoting.TxACKPacket;

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
    
    public static TxResponse parseFrom(TxACKPacket txAck) {
        TxResponse txResponse = new TxResponse();
        txResponse.setMessageId(txAck.getMessageId());
        txResponse.setStatus(txAck.getStatus());
        txResponse.setFeedback(txAck.getFeedback());
        return txResponse;
    }

}

package org.kiteq.client.message;

import org.kiteq.commons.transaction.TxnStatus;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class MessageStatus {
    
    private TxnStatus status;
    private String reason;
    
    public void rollback() {
        this.status = TxnStatus.ROLLBACK;
    }
    
    public TxnStatus getStatus() {
        return status;
    }
    
    public void setStatus(TxnStatus status) {
        this.status = status;
    }
    
    public String getReason() {
        return reason;
    }
    
    public void setReason(String reason) {
        this.reason = reason;
    }

}

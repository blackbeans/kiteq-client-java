package org.kiteq.client.message;

import org.kiteq.commons.util.JsonUtils;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class SendResult {
    
    private boolean success = true;
    private String messageId;
    private String errorMessage;
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return JsonUtils.toJSON(this);
    }

}

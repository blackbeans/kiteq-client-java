package org.kiteq.remoting.client;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class InnerSendResult {
    
    private boolean success;
    private String messageId;
    private String errorMessage;
    private byte[] serverResponse;
    
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
    
    public byte[] getServerResponse() {
        return serverResponse;
    }
    
    public void setServerResponse(byte[] serverResponse) {
        this.serverResponse = serverResponse;
    }
    
}

package org.kiteq.remoting.response;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class KiteResponse {
    
    private String requestId;
    private Object model;
    
    public KiteResponse(String requestId, Object model) {
        this.requestId = requestId;
        this.model = model;
    }
    
    public String getRequestId() {
        return requestId;
    }
    
    public Object getModel() {
        return model;
    }
}

package org.kiteq.remoting.frame;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class KiteResponse {
    
    private String requestId;
    private Object model;
    
    public String getRequestId() {
        return requestId;
    }
    
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
    
    public Object getModel() {
        return model;
    }
    
    public void setModel(Object model) {
        this.model = model;
    }

}

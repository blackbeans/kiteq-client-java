package org.kiteq.remoting.response;

/**
 * @author gaofeihang
 * @since Feb 12, 2015
 */
public class KiteResponse {

    private int requestId;
    private Object model;

    public KiteResponse(int requestId, Object model) {
        this.requestId = requestId;
        this.model = model;
    }

    public int getRequestId() {
        return requestId;
    }
    
    public Object getModel() {
        return model;
    }
}

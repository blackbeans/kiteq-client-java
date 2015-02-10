package org.kiteq.commons.message;

import org.kiteq.commons.util.JsonUtils;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class StringMessage extends Message {

    private static final long serialVersionUID = 1L;
    
    private String body = "";
    
    public String getBody() {
        return body;
    }
    
    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((body == null) ? 0 : body.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StringMessage other = (StringMessage) obj;
        if (body == null) {
            if (other.body != null) {
                return false;
            }
        } else if (!body.equals(other.body)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return JsonUtils.toJSON(this);
    }

}

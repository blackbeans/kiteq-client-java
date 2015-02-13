package org.kiteq.commons.message;

import java.util.Arrays;

import org.kiteq.commons.util.JsonUtils;

/**
 * @author gaofeihang
 * @since Feb 10, 2015
 */
public class BytesMessage extends Message {

    private static final long serialVersionUID = 1L;
    
    private byte[] body;
    
    public byte[] getBody() {
        return body;
    }
    
    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Arrays.hashCode(body);
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
        BytesMessage other = (BytesMessage) obj;
        if (!Arrays.equals(body, other.body)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return JsonUtils.toJSON(this);
    }
    
}

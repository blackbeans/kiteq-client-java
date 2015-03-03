package org.kiteq.client.message;

import org.kiteq.commons.util.ByteArrayUtils;
import org.kiteq.protocol.Protocol;
import org.kiteq.protocol.KiteRemoting.Header;

/**
 * @author gaofeihang
 * @since Mar 3, 2015
 */
public class Message {
    
    private Header header;
    private byte bodyType;
    private String bodyString;
    private byte[] bodyBytes;
    
    public Header getHeader() {
        return header;
    }
    
    public void setHeader(Header header) {
        this.header = header;
    }
    
    public boolean isStringMessage() {
        return bodyType == Protocol.CMD_STRING_MESSAGE;
    }
    
    public boolean isBytesMessage() {
        return bodyType == Protocol.CMD_BYTES_MESSAGE;
    }
    
    public void setBodyType(byte bodyType) {
        this.bodyType = bodyType;
    }
    
    public String getBodyString() {
        return bodyString;
    }
    
    public void setBodyString(String bodyString) {
        this.bodyString = bodyString;
    }
    
    public byte[] getBodyBytes() {
        return bodyBytes;
    }
    
    public void setBodyBytes(byte[] bodyBytes) {
        this.bodyBytes = bodyBytes;
    }
    
    @Override
    public String toString() {
        if (isStringMessage()) {
            return header.toString() + ", " + bodyType + ", " + bodyString;
        } else {
            return header.toString() + ", " + bodyType + ", " + ByteArrayUtils.prettyPrint(bodyBytes);
        }
    }

}

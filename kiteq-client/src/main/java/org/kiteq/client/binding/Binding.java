package org.kiteq.client.binding;

/**
 * @author gaofeihang
 * @since Mar 2, 2015
 */
public class Binding {
    
    public static final byte BIND_DIRECT = 0;
    public static final byte BIND_REGX = 0;
    public static final byte BIND_FANOUT = 0;

    public static final String BIND_VERSION="1.0.0";

    private String groupId;
    private String topic;
    private String messageType;
    private byte bindType;
    private String version;
    private int watermark;
    private boolean persistent;
    
    public String getGroupId() {
        return groupId;
    }
    
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public String getMessageType() {
        return messageType;
    }
    
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }
    
    public byte getBindType() {
        return bindType;
    }
    
    public void setBindType(byte bindType) {
        this.bindType = bindType;
    }
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    public int getWatermark() {
        return watermark;
    }
    public void setWatermark(int watermark) {
        this.watermark = watermark;
    }
    
    public boolean isPersistent() {
        return persistent;
    }
    
    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }
    
    public static Binding bindDirect(String groupId, String topic, String messageType, int watermark, boolean persistent) {
        Binding b = new Binding();
        b.setGroupId(groupId);
        b.setTopic(topic);
        b.setMessageType(messageType);
        b.setWatermark(watermark);
        b.setPersistent(persistent);
        b.setVersion(BIND_VERSION);
        b.setBindType(BIND_DIRECT);
        return b;
    }

}

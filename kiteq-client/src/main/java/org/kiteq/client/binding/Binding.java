package org.kiteq.client.binding;

/**
 * @author gaofeihang
 * @since Mar 2, 2015
 */
public class Binding {

    public static final byte BIND_DIRECT = 0;
    public static final byte BIND_REGX = 1;
    public static final byte BIND_FANOUT = 2;

    public static final String BIND_VERSION = "1.0.0";

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

    /**
     * 直接订阅
     * @param groupId
     * @param topic
     * @param messageType
     * @param watermark
     * @param persistent
     * @return
     */
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


    /**
     * 根据正则订阅
     * @param groupId
     * @param topic
     * @param regex
     * @param watermark
     * @param persistent
     * @return
     */
    public static Binding bindRegex(String groupId, String topic, String regex, int watermark, boolean persistent) {
        Binding b = new Binding();
        b.setGroupId(groupId);
        b.setTopic(topic);
        b.setMessageType(regex);
        b.setWatermark(watermark);
        b.setPersistent(persistent);
        b.setVersion(BIND_VERSION);
        b.setBindType(BIND_REGX);
        return b;
    }


    /**
     * 全局订阅
     * @param groupId
     * @param topic
     * @param watermark
     * @param persistent
     * @return
     */
    public static Binding bindFanout(String groupId, String topic,int watermark, boolean persistent) {
        Binding b = new Binding();
        b.setGroupId(groupId);
        b.setTopic(topic);
        b.setMessageType("*");
        b.setWatermark(watermark);
        b.setPersistent(persistent);
        b.setVersion(BIND_VERSION);
        b.setBindType(BIND_FANOUT);
        return b;
    }

}

package org.kiteq.commons.exception;

/**
 * KiteQClient的异常
 * Created by blackbeans on 21/10/2016.
 */
public class KiteQClientException extends Exception {

    private String topic;

    private String messageType;

    private String messageId;


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

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public KiteQClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public KiteQClientException(String topic, String messageType,
                                String messageId, Throwable t) {
        super(t);
        this.topic = topic;
        this.messageType = messageType;
        this.messageId = messageId;
    }



    @Override
    public String toString() {
        return "KiteQClientException{" +
                "topic='" + topic + '\'' +
                ", messageType='" + messageType + '\'' +
                ", messageId='" + messageId + '\'' +
                ", cause='"+super.toString()+'\''+
                '}';
    }
}

package org.kiteq.commons.exception;

/**
 * KiteQClient的异常
 * Created by blackbeans on 21/10/2016.
 */
public class KiteQClientException extends Exception {

    private String topic;

    private String messageType;

    private String messageId;

    public KiteQClientException(String topic, String messageType, String messageId) {
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

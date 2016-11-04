package org.kiteq.protocol;

import java.util.UUID;

/**
 * Created by blackbeans on 04/11/2016.
 */
public class Demo {

    public static void main(String[] args) {
        KiteRemoting.StringMessage msg =  buildMessage("trade","test","pay-succ-0","ddddd");
        msg = msg.toBuilder().setHeader(msg.getHeader().toBuilder().setCommit(false)).build();
        System.out.println(msg);
    }

    public static KiteRemoting.StringMessage
    buildMessage(String topic, String groupId, String messageType, String body) {
        KiteRemoting.Header header = KiteRemoting.Header.newBuilder().setMessageId(UUID.randomUUID().toString()
                .replace("-", "")).setTopic(topic).setMessageType(messageType).setExpiredTime(-1L).setDeliverLimit(100)
                .setGroupId(groupId).setCommit(true).setFly(false).build();
        return KiteRemoting.StringMessage.newBuilder().setHeader(header).setBody(body).build();
    }
}

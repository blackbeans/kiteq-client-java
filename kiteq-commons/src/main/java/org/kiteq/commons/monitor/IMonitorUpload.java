package org.kiteq.commons.monitor;



import java.util.Map;

/**
 * 增加监控上报
 * Created by blackbeans on 6/29/16.
 */
public interface IMonitorUpload {

    void sendMonitorData(String groupId, String host, Map<String, Object> dataPacket,long timestamp);
}

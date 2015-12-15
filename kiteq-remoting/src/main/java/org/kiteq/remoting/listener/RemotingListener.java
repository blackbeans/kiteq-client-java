package org.kiteq.remoting.listener;



import org.kiteq.protocol.packet.KitePacket;

/**
 * @author gaofeihang
 * @since Feb 13, 2015
 */
public interface RemotingListener {

    KitePacket txAckReceived(KitePacket packet);

    KitePacket bytesMessageReceived(KitePacket packet);

    KitePacket stringMessageReceived(KitePacket packet);

}

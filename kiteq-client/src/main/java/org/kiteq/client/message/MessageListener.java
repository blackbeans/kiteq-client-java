package org.kiteq.client.message;


import org.kiteq.commons.exception.KiteQClientException;

/**
 * @author gaofeihang
 * @since Feb 28, 2015
 */
public interface MessageListener {

    /**
     * 接受正常消息的回调
     *
     * 只有在明确返回true的情况下才认为处理成功
     * 如果返回false或者抛出异常的情况认为处理失败等待重投
     *
     * @param message
     * @return
     * @throws KiteQClientException
     */
    boolean onMessage(Message message) throws KiteQClientException;

    /**
     * 处理事务消息的回调
     * 只有在tx.commit()的情况才是返回成功
     * tx.rollback()或者抛出异常的的情况需要回滚
     * @param tx
     * @throws KiteQClientException
     */
    void onMessageCheck(TxResponse tx) throws KiteQClientException;

}

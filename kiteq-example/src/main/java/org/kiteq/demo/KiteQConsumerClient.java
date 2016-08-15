package org.kiteq.demo;

import com.google.protobuf.ByteString;
import org.kiteq.client.DefaultKiteClient;
import org.kiteq.client.binding.Binding;
import org.kiteq.client.message.Message;
import org.kiteq.client.message.MessageListener;
import org.kiteq.client.message.SendResult;
import org.kiteq.client.message.TxResponse;
import org.kiteq.protocol.KiteRemoting;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.kiteq.protocol.KiteRemoting.Header;

/**
 * blackbeans at 2015-04-08.
 */
public class KiteQConsumerClient {


    public static void main(String[] args) throws Exception {

        //设置Consumer接收消息的Binding
        List<Binding> bindings = new ArrayList<Binding>();
        bindings.add(Binding.bindDirect("s-kiteq-group", "trade", "pay-succ", 6000, true));

        DefaultKiteClient client = new DefaultKiteClient();
        client.setZkHosts("localhost:2181");
        client.setBindings(bindings);
        client.setGroupId("s-kiteq-group");
        client.setSecretKey("default");

        //为了避免启动时,所在服务进程资源未初始化到最后状态
        //可以设置预热时间。KiteQ会逐步在规定时间内放量到100%
        client.setWarmingupSeconds(60);
        client.setListener(new MessageListener() {
            @Override
            public boolean onMessage(Message message) {
                /**
                 *  Consumer接收消息的入口
                 *   之后再显示返回 true的情况下,才认为消息消费成功
                 *   否则 抛异常或者false的情况下,KiteQ会重投
                 */

                /**
                 * 处理业务逻辑
                 */

                String topic = message.getHeader().getTopic();
                String messageType = message.getHeader().getMessageType();

                //根据自己的业务定义的Body反序列化为业务对象进行处理
                String body = ByteString.copyFrom(message.getBodyBytes()).toStringUtf8();

                return true;
            }

            @Override
            public void onMessageCheck(TxResponse tx) {
                //ignored ,作为消费方不用实现

            }
        });

        //不要忘记init
        client.init();

    }
}

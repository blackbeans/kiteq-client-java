# A Java Client for KiteQ
* More Details: https://github.com/blackbeans/kiteq

## Development

    git clone https://github.com/blackbeans/kiteq-client-java.git kiteq
    
## Build

    cd kiteq
    mvn clean package -Dmaven.test.skip
    
## Benchmark

    cd kiteq-benchmark/target
    tar -xzvf kiteq-benchmark-make-assembly.tar.gz
    sh kite_benchmark_consumer.sh
    sh kite_benchmark_producer.sh

## API

### Producer Example

    import java.util.UUID;
    
    import org.kiteq.client.KiteClient;
    import org.kiteq.client.impl.DefaultKiteClient;
    import org.kiteq.client.message.ListenerAdapter;
    import org.kiteq.client.message.SendResult;
    import org.kiteq.client.message.TxResponse;
    import org.kiteq.commons.util.JsonUtils;
    import org.kiteq.protocol.KiteRemoting.Header;
    import org.kiteq.protocol.KiteRemoting.StringMessage;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    
    public class KiteProducer {
        private static final Logger logger = LoggerFactory.getLogger(KiteProducer.class);
        
        private static final String ZK_ADDR = "localhost:2181";
        private static final String GROUP_ID = "pb-mts-test";
        private static final String SECRET_KEY = "123456";
        private static final String TOPOIC = "trade";
        
        private KiteClient producer;
        
        public KiteProducer() {
            producer = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY, new ListenerAdapter() {
                @Override
                public void onMessageCheck(TxResponse response) {
                    logger.warn(JsonUtils.prettyPrint(response));
                    response.commit();
                }
            });
            producer.setPublishTopics(new String[] { TOPOIC });
        }
        
        private StringMessage buildMessage() {
            String messageId = UUID.randomUUID().toString();
            Header header = Header.newBuilder()
                    .setMessageId(messageId)
                    .setTopic(TOPOIC)
                    .setMessageType("pay-succ")
                    .setExpiredTime(System.currentTimeMillis())
                    .setDeliverLimit(-1)
                    .setGroupId("go-kite-test")
                    .setCommit(false).build();
            
            StringMessage message = StringMessage.newBuilder()
                    .setHeader(header)
                    .setBody("echo").build();
            return message;
        }
        
        public void start() {
            producer.start();
            SendResult result = producer.sendStringMessage(buildMessage());
            logger.warn("Send result: {}", result);
            producer.close();
        }
        
        public static void main(String[] args) {
            System.setProperty("kiteq.appName", "Producer");
            new KiteProducer().start();
        }
    }

### Consumer Example

    import org.kiteq.binding.Binding;
    import org.kiteq.client.KiteClient;
    import org.kiteq.client.impl.DefaultKiteClient;
    import org.kiteq.client.message.ListenerAdapter;
    import org.kiteq.client.message.Message;
    import org.kiteq.commons.stats.KiteStats;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    
    public class KiteConsumer {
        private static final Logger logger = LoggerFactory.getLogger(KiteConsumer.class);
        
        private static final String ZK_ADDR = "localhost:2181";
        private static final String GROUP_ID = "s-mts-test";
        private static final String SECRET_KEY = "123456";
        
        private KiteClient consumer;
        
        public KiteConsumer() {
            consumer = new DefaultKiteClient(ZK_ADDR, GROUP_ID, SECRET_KEY, new ListenerAdapter() {
                @Override
                public boolean onMessage(Message message) {
                    logger.warn("recv: {}", message);
                    return true;
                }
            });
            consumer.setBindings(new Binding[] { Binding.bindDirect(GROUP_ID, "trade", "pay-succ", 1000, true) });
        }
        
        public void start() {
            consumer.start();
            KiteStats.close();
        }
        
        public static void main(String[] args) {
            System.setProperty("kiteq.appName", "Consumer");
            new KiteConsumer().start();
        }
    }
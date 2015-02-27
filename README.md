## A Java Client for KiteQ
* More Details: https://github.com/blackbeans/kiteq

### Development

    git clone https://github.com/blackbeans/kiteq-client-java.git kiteq
    
### Build

    cd kiteq
    mvn clean package -Dmaven.test.skip
    
### Benchmark

    cd kiteq-benchmark/target
    tar -xzvf kiteq-benchmark-make-assembly.tar.gz
    sh kite_benchmark_consumer.sh
    sh kite_benchmark_producer.sh

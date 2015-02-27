## A Java Client for KiteQ
* More Details: https://github.com/blackbeans/kiteq

### Development

    git clone https://github.com/blackbeans/kiteq-client-java.git kiteq
    
### Build

    cd kiteq
    mvn clean package -Dmaven.test.skip
    
### Benchmark

    cd kiteq-benchmark
    tar -xzvf kiteq-benchmark-make-assembly.tar.gz
    
#### Producer

    sh kite_benchmark_producer.sh -t 10

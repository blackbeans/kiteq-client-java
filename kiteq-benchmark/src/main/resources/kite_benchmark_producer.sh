jvm_args="-Xmx2048m -Xms1024m -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSCompactAtFullCollection -XX:MaxTenuringThreshold=10 -XX:-UseAdaptiveSizePolicy -XX:PermSize=256M -XX:MaxPermSize=512M -XX:SurvivorRatio=3 -XX:NewRatio=2 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
java -cp .:conf/*:lib/* ${jvm_args} org.kiteq.benchmark.KiteProducerBenchmark $1 $2 $3 $4 $5 $6

.PHONY:
	add-graphite

add-graphite:
	cp ./build-target/opt/flink-metrics-graphite-1.10-SNAPSHOT.jar ./build-target/lib/

cp-conf:
	cp ./flink-conf.yaml ./build-target/conf

start:
	./build-target/bin/start-cluster.sh

stop:
	./build-target/bin/stop-cluster.sh


run-example:
	 ./build-target/bin/flink run ./flink-examples/flink-examples-streaming/target/MiniBatchWordCount.jar

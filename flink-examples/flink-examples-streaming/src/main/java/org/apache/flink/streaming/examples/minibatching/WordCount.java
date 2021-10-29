package org.apache.flink.streaming.examples.minibatching;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStateBackend(new RocksDBStateBackend("file:///tmp/flink_rocks.db"));

        DataStream<String> text = env.socketTextStream("localhost", 8765);

        DataStream<Tuple2<String, Integer>> counts = text.keyBy(value -> value)
                .process(new MiniBatchWordCount())
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        counts.print();
        env.execute("Streaming WordCount");
    }
}


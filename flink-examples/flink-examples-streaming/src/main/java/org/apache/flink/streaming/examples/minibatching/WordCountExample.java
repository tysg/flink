package org.apache.flink.streaming.examples.minibatching;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class WordCountExample extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {


    private transient ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<Integer>("count", Types.INT);

        countState = getRuntimeContext().getState(countDescriptor);
    }


    @Override
    public void processElement(
            String word,
            Context context,
            Collector<Tuple2<String, Integer>> collector) throws Exception {

        Integer count = countState.value();
        if (count != null) {
            count += 1;
        } else {
            count = 1;
        }

        countState.update(count);

        collector.collect(new Tuple2<>(word, count));
    }

}

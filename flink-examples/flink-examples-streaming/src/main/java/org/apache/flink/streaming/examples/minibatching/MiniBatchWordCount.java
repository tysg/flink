package org.apache.flink.streaming.examples.minibatching;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MiniBatchWordCount extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {


    private transient ValueState<Integer> countState;
    private HashMap<String, ArrayList<String>> buffer;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<Integer>("count", Types.INT);

        countState = getRuntimeContext().getState(countDescriptor);
    }

    @Override
    public void processElement(String s, KeyedProcessFunction<String, String, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {

    }

    public void processBundle(
            Context context,
            Collector<Tuple2<String, Integer>> collector) throws Exception {

        for (Map.Entry<String, ArrayList<String>> entry : buffer.entrySet()) {
            String key = entry.getKey();
            ArrayList<String> values = entry.getValue();
            // TODO: how to access the value state by key????
			// set key via context
			((StreamingRuntimeContext) getRuntimeContext()).setCurrentKey("hello");
            Integer count = countState.value();
            for (String s : values) {
                if (count != null) {
                    count += 1;
                } else {
                    count = 1;
                }
            }
            countState.update(count);

            collector.collect(new Tuple2<>(key, count));
        }
    }

}

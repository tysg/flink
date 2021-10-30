package org.apache.flink.streaming.examples.minibatching;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MiniBatchWordCount extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {

	// config
	private final long MINI_BATCH_ALLOW_LATENCY = 5 * 1000;


	private transient ValueState<Integer> countState;
	private HashMap<String, ArrayList<String>> buffer;
	private boolean isScheduledBundle;

	// metrics
	private transient Counter counter;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<Integer>("count", Types.INT);

		countState = getRuntimeContext().getState(countDescriptor);
		buffer = new HashMap<>();
		isScheduledBundle = false;

		this.counter = getRuntimeContext().getMetricGroup().counter("myCounter");
	}

	@Override
	public void processElement(String s, KeyedProcessFunction<String, String, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
		// add element to map
		if (buffer.containsKey(s)) {
			buffer.get(s).add(s);
		} else {
			ArrayList<String> l = new ArrayList<>();
			buffer.put(s, l);
		}
		if (!isScheduledBundle) {
			long scheduledTime = context.timerService().currentProcessingTime() + MINI_BATCH_ALLOW_LATENCY;
			context.timerService().registerProcessingTimeTimer(scheduledTime);
			isScheduledBundle = true;
		}
	}

	@Override
	public void onTimer(long timestamp, KeyedProcessFunction<String, String, Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
		processBundle(ctx, out);
		isScheduledBundle = false;
	}

	public void processBundle(
		Context context,
		Collector<Tuple2<String, Integer>> collector) throws Exception {

		for (Map.Entry<String, ArrayList<String>> entry : buffer.entrySet()) {
			String key = entry.getKey();
			ArrayList<String> values = entry.getValue();
			// set key via context
			((StreamingRuntimeContext) getRuntimeContext()).setCurrentKey(key);
			Integer count = countState.value();
			for (String s : values) {
				if (count != null) {
					count += 1;
				} else {
					count = 1;
				}
				this.counter.inc();
			}
			countState.update(count);
			collector.collect(new Tuple2<>(key, count));
		}
		buffer.clear();
	}

}

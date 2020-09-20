package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class UpdatedOperator<IN, OUT> extends AbstractUdfStreamOperator<OUT, ControlFunction>
	implements OneInputStreamOperator<IN, OUT> {


	public UpdatedOperator(ControlFunction userFunction) {
		super(userFunction);
	}


	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		this.userFunction.invokeControl(null, element.getValue());
	}
}

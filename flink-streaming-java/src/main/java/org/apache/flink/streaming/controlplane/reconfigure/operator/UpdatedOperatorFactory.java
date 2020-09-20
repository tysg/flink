package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;

public abstract class UpdatedOperatorFactory extends SimpleOperatorFactory<Object> {

	OperatorDescriptor descriptor;

	protected UpdatedOperatorFactory(StreamOperator<Object> operator) {
		super(operator);
	}

	protected UpdatedOperatorFactory(OperatorID operatorID, JobGraph jobGraph) {
		this(null);
	}


	@Override
	public StreamOperator<Object> getOperator() {
		return create(null);
	}

	public abstract UpdatedOperator<Object, Object> create(ControlFunction function);


}

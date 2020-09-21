package org.apache.flink.streaming.controlplane.jobgraph;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

public interface JobGraphUpdateOperator {

	void updateOperator(OperatorID operatorID,  StreamOperatorFactory operatorFactory) throws Exception;

}

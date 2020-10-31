package org.apache.flink.streaming.controlplane.jobgraph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.topology.VertexID;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

public interface JobGraphOperatorUpdate {

	/**
	 *
	 * @param operatorID
	 * @param operatorFactory
	 * @param <OUT>
	 * @return the id of updated job vertex
	 * @throws Exception
	 */
	<OUT> JobVertexID updateOperator(OperatorID operatorID, StreamOperatorFactory<OUT> operatorFactory) throws Exception;

}

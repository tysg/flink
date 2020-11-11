package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.Map;

public interface JobGraphUpdater {

	/**
	 *
	 * @param operatorID
	 * @param function
	 * @param <OUT>
	 * @return
	 * @throws Exception
	 */
	<OUT> JobVertexID updateOperator(int operatorID, Function function) throws Exception;

	Map<Integer, OperatorID> getOperatorIDMap();
}

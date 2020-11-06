package org.apache.flink.streaming.controlplane.streammanager.insts;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface OperatorGraphState {
	/**
	 * Return UserFunction, StreamOperator, or StreamOperatorFactory?
	 *
	 * @param operatorID the operator id of this operator
	 * @return
	 */
	Function getUserFunction(OperatorID operatorID) throws Exception;

	/**
	 * To get how the input key state was allocated among the sub operator instance.
	 * Map(StreamEdgeId -> [OutputChannelIndex, [assigned keys]]),
	 * Multiple stream edge id means there is multiple input edges.
	 *
	 * @param operatorID the target key mapping of opeartor we want to know
	 * @return A map from streamEdgeId and the key mapping of this stream edge, the map value is a list o list
	 * representing the output channel with its assigned keys.
	 * @throws Exception
	 */
	Map<OperatorID, List<List<Integer>>> getKeyStateAllocation(OperatorID operatorID) throws Exception;

	/**
	 * Return how the key mapping to down stream operator
	 *
	 * @param operatorID the target key mapping of opeartor we want to know
	 * @return A map from streamEdgeId and the key mapping of this stream edge, the map value is a list o list
	 * representing the output channel with its assigned keys. Map(StreamEdgeId -> [OutputChannelIndex, [assigned keys]])
	 * @throws Exception
	 */
	Map<OperatorID, List<List<Integer>>> getKeyMapping(OperatorID operatorID) throws Exception;

	/**
	 * Return the parallilism of operator with given operator id
	 * @param operatorID
	 * @return
	 */
	int getParallelism(OperatorID operatorID);

	Iterator<OperatorDescriptor> getAllOperatorDescriptor();
}

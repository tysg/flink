package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.StreamRelatedInstanceFactory;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;
import scala.Int;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import java.util.stream.Collectors;


public abstract class AbstractCoordinator implements PrimitiveOperation {

	protected JobGraph jobGraph;
	protected ExecutionGraph executionGraph;
	protected ClassLoader userCodeClassLoader;

	private StreamRelatedInstanceFactory streamRelatedInstanceFactory;

	private JobGraphUpdater updater;
	protected StreamJobExecutionPlan heldExecutionPlan;
	protected Map<Integer, OperatorID> operatorIDMap;


	protected AbstractCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;
		this.userCodeClassLoader = executionGraph.getUserClassLoader();
	}

	public void setStreamRelatedInstanceFactory(StreamRelatedInstanceFactory streamRelatedInstanceFactory) {
		heldExecutionPlan = streamRelatedInstanceFactory.createExecutionPlan(jobGraph, executionGraph, userCodeClassLoader);
		updater = streamRelatedInstanceFactory.createJobGraphUpdater(jobGraph, userCodeClassLoader);
		operatorIDMap = updater.getOperatorIDMap();
		this.streamRelatedInstanceFactory = streamRelatedInstanceFactory;
	}

	public StreamJobExecutionPlan getHeldExecutionPlanCopy() {
		StreamJobExecutionPlan executionPlan = streamRelatedInstanceFactory.createExecutionPlan(jobGraph, executionGraph, userCodeClassLoader);
		for (Iterator<OperatorDescriptor> it = executionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			OperatorDescriptor heldDescriptor = heldExecutionPlan.getOperatorDescriptorByID(descriptor.getOperatorID());
			// make sure udf share the same reference so we could identity the change if any
			descriptor.setUdf(heldDescriptor.getUdf());
		}
		return executionPlan;
	}

	protected JobVertexID rawVertexIDToJobVertexID(int rawID) {
		OperatorID operatorID = operatorIDMap.get(rawID);
		if (operatorID == null) {
			return null;
		}
		for (JobVertex vertex : jobGraph.getVertices()) {
			if (vertex.getOperatorIDs().contains(operatorID)) {
				return vertex.getID();
			}
		}
		return null;
	}

	@Override
	public final CompletableFuture<Void> prepareExecutionPlan(StreamJobExecutionPlan jobExecutionPlan) {
		for (Iterator<OperatorDescriptor> it = jobExecutionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			int operatorID = descriptor.getOperatorID();
			OperatorDescriptor heldDescriptor = heldExecutionPlan.getOperatorDescriptorByID(operatorID);
			int oldParallelism = heldDescriptor.getParallelism();
			// loop until all change in this operator has been detected and sync
			List<Integer> changes = analyzeOperatorDifference(heldDescriptor, descriptor);
			for (int changedPosition : changes) {
				try {
					switch (changedPosition) {
						case UDF:
							heldDescriptor.setUdf(descriptor.getUdf());
							updater.updateOperator(operatorID, descriptor.getUdf());
							break;
						case PARALLELISM:
							heldDescriptor.setParallelism(descriptor.getParallelism());
							// next update job graph
							JobVertexID jobVertexID = rawVertexIDToJobVertexID(heldDescriptor.getOperatorID());
							JobVertex vertex = jobGraph.findVertexByID(jobVertexID);
							vertex.setParallelism(heldDescriptor.getParallelism());
							break;
						case KEY_STATE_ALLOCATION:
							heldDescriptor.setKeyStateAllocation(descriptor.getKeyStateAllocation());
							updateKeyset(heldDescriptor);
							break;
						case KEY_MAPPING:
							// update key set will indirectly update key mapping, so we ignore this type of detected change here
							break;
						case NO_CHANGE:
					}
				} catch (Exception e) {
					e.printStackTrace();
					return FutureUtils.completedExceptionally(e);
				}
			}
			// update execution graph
			if (changes.contains(PARALLELISM)) {
				rescaleExecutionGraph(heldDescriptor.getOperatorID(), oldParallelism);
			}
		}
		// TODO: suspend checking StreamJobExecution for scale out
//		final StreamJobExecutionPlan executionPlan = getHeldExecutionPlanCopy();
//		for (Iterator<OperatorDescriptor> it = executionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
//			OperatorDescriptor descriptor = it.next();
//			OperatorDescriptor held = heldExecutionPlan.getOperatorDescriptorByID(descriptor.getOperatorID());
//			for (int changedPosition : analyzeOperatorDifference(held, descriptor)) {
//				System.out.println("change no work:" + changedPosition);
//				return FutureUtils.completedExceptionally(new Exception("change no work:" + changedPosition));
//			}
//		}
		return CompletableFuture.completedFuture(null);
	}

	private void rescaleExecutionGraph(int rawVertexID, int oldParallelism) {
		// scale up given ejv, update involved edges & partitions
		ExecutionJobVertex targetVertex = executionGraph.getJobVertex(rawVertexIDToJobVertexID(rawVertexID));
		JobVertex targetJobVertex = jobGraph.findVertexByID(rawVertexIDToJobVertexID(rawVertexID));
		Preconditions.checkNotNull(targetVertex, "can not found target vertex");
		// todo how about scale in
		if (oldParallelism < targetJobVertex.getParallelism()) {
			targetVertex.scaleOut(executionGraph.getRpcTimeout(), executionGraph.getGlobalModVersion(), System.currentTimeMillis());
		}
		List<JobVertexID> updatedDownstream = heldExecutionPlan.getOperatorDescriptorByID(rawVertexID)
			.getChildren().stream()
			.map(child -> rawVertexIDToJobVertexID(child.getOperatorID()))
			.collect(Collectors.toList());
		for (JobVertexID downstreamID : updatedDownstream) {
			ExecutionJobVertex downstream = executionGraph.getJobVertex(downstreamID);
			if (downstream != null) {
				downstream.reconnectWithUpstream(targetVertex.getProducedDataSets());
			}
		}
		executionGraph.updateNumOfTotalVertices();
	}

	private void updateKeyset(OperatorDescriptor heldDescriptor) {
		Map<Integer, List<Integer>> partionAssignment = new HashMap<>();
		for (List<List<Integer>> one : heldDescriptor.getKeyStateAllocation().values()) {
			for (int i = 0; i < one.size(); i++) {
				partionAssignment.put(i, one.get(i));
			}
			updater.repartition(rawVertexIDToJobVertexID(heldDescriptor.getOperatorID()), partionAssignment);
			// we think each operator will only have one key set
			break;
		}
	}

	private List<Integer> analyzeOperatorDifference(OperatorDescriptor self, OperatorDescriptor modified) {
		List<Integer> results = new LinkedList<>();
		if (self.getUdf() != modified.getUdf()) {
			results.add(UDF);
		}
		if (self.getParallelism() != modified.getParallelism()) {
			results.add(PARALLELISM);
		}
		if (compare(self.getKeyStateAllocation(), modified.getKeyStateAllocation())) {
			results.add(KEY_STATE_ALLOCATION);
		}
		if (compare(self.getKeyMapping(), modified.getKeyMapping())) {
			results.add(KEY_MAPPING);
		}
		Collections.sort(results);
		return results;
	}

	/**
	 * @param map1
	 * @param map2
	 * @return true if there are different
	 */
	private boolean compare(Map<Integer, List<List<Integer>>> map1, Map<Integer, List<List<Integer>>> map2) {
		if (map1.size() != map2.size()) {
			return true;
		}
		for (Integer integer : map1.keySet()) {
			List<List<Integer>> lists = map2.get(integer);
			if (lists == null || compare(map1.get(integer), lists)) {
				return true;
			}
		}
		return false;
	}

	private boolean compare(List<List<Integer>> list1, List<List<Integer>> list2) {
		if (list1.size() != list2.size()) {
			return true;
		}
		for (int i = 0; i < list1.size(); i++) {
			if (compareIntList(list1.get(i), list2.get(i))) {
				return true;
			}
		}
		return false;
	}

	private boolean compareIntList(List<Integer> list1, List<Integer> list2) {
		if (list1.size() != list2.size()) {
			return true;
		}
		return !Arrays.equals(
			list1.stream().sorted().toArray(),
			list2.stream().sorted().toArray());
	}

	@Override
	@Deprecated
	public CompletableFuture<Acknowledge> updateFunction(JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID) {
		throw new UnsupportedOperationException();
	}

	// the value means priority, the higher, the later should be resolve
	private static final int UDF = 0;
	private static final int PARALLELISM = 1;
	private static final int KEY_STATE_ALLOCATION = 2;
	private static final int KEY_MAPPING = 3;
	private static final int NO_CHANGE = 4;

}

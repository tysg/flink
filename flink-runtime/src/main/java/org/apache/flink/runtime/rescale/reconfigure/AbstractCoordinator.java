package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.StreamRelatedInstanceFactory;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;

import java.util.*;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractCoordinator implements PrimitiveOperation {

	protected JobGraph jobGraph;
	protected ExecutionGraph executionGraph;
	protected ClassLoader userCodeClassLoader;

	private StreamRelatedInstanceFactory streamRelatedInstanceFactory;

	private JobGraphUpdater updater;
	private StreamJobExecutionPlan heldExecutionPlan;
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
			// loop until all change in this operator has been detected and sync
			ChangedPosition changedPosition;
			do {
				changedPosition = analyzeOperatorDifference(heldDescriptor, descriptor);
				try {
					switch (changedPosition) {
						case UDF:
							heldDescriptor.setUdf(descriptor.getUdf());
							updater.updateOperator(operatorID, descriptor.getUdf());
							break;
						case KEY_STATE_ALLOCATION:
							heldDescriptor.setKeyStateAllocation(descriptor.getKeyStateAllocation());
							updateKeyset(heldDescriptor);
							break;
						case PARALLELISM:
							heldDescriptor.setParallelism(descriptor.getParallelism());
							// next update job graph and execution graph
							break;
						case NO_CHANGE:
					}

				} catch (Exception e) {
					e.printStackTrace();
					return FutureUtils.completedExceptionally(e);
				}
			} while (changedPosition != ChangedPosition.NO_CHANGE);
		}
		return CompletableFuture.completedFuture(null);
	}

	private void updateKeyset(OperatorDescriptor heldDescriptor) {
		Map<Integer, List<Integer>> partionAssignment = new HashMap<>();
		for (List<List<Integer>> one : heldDescriptor.getKeyStateAllocation().values()) {
			for (int i = 0; i < one.size(); i++) {
				partionAssignment.put(i, one.get(i));
			}
			updater.repartition(rawVertexIDToJobVertexID(heldDescriptor.getOperatorID()), partionAssignment);
			break;
		}
	}

	private ChangedPosition analyzeOperatorDifference(OperatorDescriptor self, OperatorDescriptor modified) {
		if (self.getUdf() != modified.getUdf()) {
			return ChangedPosition.UDF;
		}
		if (compare(self.getKeyStateAllocation(), modified.getKeyStateAllocation())) {
			return ChangedPosition.KEY_STATE_ALLOCATION;
		}
		if (self.getParallelism() != modified.getParallelism()) {
			return ChangedPosition.PARALLELISM;
		}
		return ChangedPosition.NO_CHANGE;
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
		for (int i = 0; i < list1.size(); i++) {
			if (!list1.get(i).equals(list2.get(i))) {
				return true;
			}
		}
		return false;
	}

	@Override
	@Deprecated
	public CompletableFuture<Acknowledge> updateFunction(JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID) {
		throw new UnsupportedOperationException();
	}

	enum ChangedPosition {
		UDF,
		KEY_MAPPING,
		KEY_STATE_ALLOCATION,
		PARALLELISM,
		NO_CHANGE
	}

}

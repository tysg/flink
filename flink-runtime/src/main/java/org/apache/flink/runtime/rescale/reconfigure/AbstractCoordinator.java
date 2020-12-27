package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.StreamRelatedInstanceFactory;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptorVisitor;
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


public abstract class AbstractCoordinator implements PrimitiveOperation<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> {

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
			// make sure udf and other control attributes share the same reference so we could identity the change if any
			OperatorDescriptor.ApplicationLogic heldAppLogic =
				OperatorDescriptorVisitor.attachOperator(heldDescriptor).getApplicationLogic();
			OperatorDescriptor.ApplicationLogic appLogicCopy =
				OperatorDescriptorVisitor.attachOperator(descriptor).getApplicationLogic();
			heldAppLogic.copyTo(appLogicCopy);
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
	public final CompletableFuture<Map<Integer, Map<Integer, Diff>>> prepareExecutionPlan(StreamJobExecutionPlan jobExecutionPlan) {
		Map<Integer, Map<Integer, Diff>> differenceMap = new HashMap<>();
		for (Iterator<OperatorDescriptor> it = jobExecutionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			int operatorID = descriptor.getOperatorID();
			OperatorDescriptor heldDescriptor = heldExecutionPlan.getOperatorDescriptorByID(operatorID);
			int oldParallelism = heldDescriptor.getParallelism();
			// loop until all change in this operator has been detected and sync
			List<Integer> changes = analyzeOperatorDifference(heldDescriptor, descriptor);
			Map<Integer, Diff> difference = new HashMap<>();
			differenceMap.put(operatorID, difference);
			for (int changedPosition : changes) {
				try {
					switch (changedPosition) {
						case UDF:
							OperatorDescriptor.ApplicationLogic heldAppLogic =
								OperatorDescriptorVisitor.attachOperator(heldDescriptor).getApplicationLogic();
							OperatorDescriptor.ApplicationLogic modifiedAppLogic =
								OperatorDescriptorVisitor.attachOperator(descriptor).getApplicationLogic();
							modifiedAppLogic.copyTo(heldAppLogic);
							updater.updateOperator(operatorID, heldAppLogic);
							difference.put(UDF, ExecutionLogic.UDF);
							break;
						case PARALLELISM:
							heldDescriptor.setParallelism(descriptor.getParallelism());
							// next update job graph
							JobVertexID jobVertexID = rawVertexIDToJobVertexID(heldDescriptor.getOperatorID());
							JobVertex vertex = jobGraph.findVertexByID(jobVertexID);
							vertex.setParallelism(heldDescriptor.getParallelism());
//							difference.add(PARALLELISM);
							break;
						case KEY_STATE_ALLOCATION:
							difference.put(
								KEY_STATE_ALLOCATION,
								new RemappingAssignment(descriptor.getKeyStateAllocation(), heldDescriptor.getKeyStateAllocation())
							);
							heldDescriptor.setKeySet(descriptor.getKeyStateAllocation());
							updateKeyset(heldDescriptor);
							break;
						case KEY_MAPPING:
							// update key set will indirectly update key mapping, so we ignore this type of detected change here
							difference.put(KEY_MAPPING, ExecutionLogic.KEY_MAPPING);
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
		return CompletableFuture.completedFuture(differenceMap);
	}

	private void rescaleExecutionGraph(int rawVertexID, int oldParallelism) {
		// scale up given ejv, update involved edges & partitions
		ExecutionJobVertex targetVertex = executionGraph.getJobVertex(rawVertexIDToJobVertexID(rawVertexID));
		JobVertex targetJobVertex = jobGraph.findVertexByID(rawVertexIDToJobVertexID(rawVertexID));
		Preconditions.checkNotNull(targetVertex, "can not found target vertex");
		int newParallelism = targetJobVertex.getParallelism();
		if (oldParallelism < newParallelism) {
			targetVertex.scaleOut(executionGraph.getRpcTimeout(), executionGraph.getGlobalModVersion(), System.currentTimeMillis(), newParallelism);
		}
		// TODO: scale in
		// TODO: throw rescale exception

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
		List<List<Integer>> one = heldDescriptor.getKeyStateAllocation();
		for (int i = 0; i < one.size(); i++) {
			partionAssignment.put(i, one.get(i));
		}
		updater.repartition(rawVertexIDToJobVertexID(heldDescriptor.getOperatorID()), partionAssignment);
	}

	private List<Integer> analyzeOperatorDifference(OperatorDescriptor self, OperatorDescriptor modified) {
		List<Integer> results = new LinkedList<>();
		OperatorDescriptor.ApplicationLogic heldAppLogic =
			OperatorDescriptorVisitor.attachOperator(self).getApplicationLogic();
		OperatorDescriptor.ApplicationLogic modifiedAppLogic =
			OperatorDescriptorVisitor.attachOperator(modified).getApplicationLogic();
		if (!heldAppLogic.equals(modifiedAppLogic)) {
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

	/**
	 *
	 * @param list1
	 * @param list2
	 * @return true if they are not equals
	 */
	static boolean compareIntList(List<Integer> list1, List<Integer> list2) {
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
	public static final int UDF = 0;
	public static final int PARALLELISM = 1;
	public static final int KEY_STATE_ALLOCATION = 2;
	public static final int KEY_MAPPING = 3;
	public static final int NO_CHANGE = 4;


	public interface Diff {
	}

	enum ExecutionLogic implements Diff {
		UDF, KEY_MAPPING
	}

}

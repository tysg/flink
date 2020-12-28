package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WorkloadsAssignmentHandler {
	// operator -> workload assignment
	private final Map<Integer, OperatorWorkloadsAssignment> heldWorkloadsAssignmentMap;
	private final Map<Integer, Map<Integer, List<Integer>>> heldExecutorMapping;

	public WorkloadsAssignmentHandler(StreamJobExecutionPlan jobExecutionPlan) {
		heldWorkloadsAssignmentMap = new HashMap<>();
		heldExecutorMapping = new HashMap<>();
		setupWorkloadsAssignmentMapFromExecutionPlan(jobExecutionPlan);
	}

	public void setupWorkloadsAssignmentMapFromExecutionPlan(StreamJobExecutionPlan heldExecutionPlan) {
//	public Map<Integer, OperatorWorkloadsAssignment> setupWorkloadsAssignmentMapFromExecutionPlan(StreamJobExecutionPlan heldExecutionPlan) {
		for (Iterator<OperatorDescriptor> it = heldExecutionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			int operatorID = descriptor.getOperatorID();
			int parallelism = descriptor.getParallelism();
			Map<Integer, List<Integer>> keyStateAllocation = descriptor.getKeyStateAllocation();
			OperatorWorkloadsAssignment operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(keyStateAllocation, parallelism);
			heldWorkloadsAssignmentMap.put(operatorID, operatorWorkloadsAssignment);
			heldExecutorMapping.put(operatorID, keyStateAllocation);
		}
//		return heldWorkloadsAssignmentMap;
	}

	public OperatorWorkloadsAssignment handleWorkloadsReallocate(int operatorId, Map<Integer, List<Integer>> executorMapping) {
		System.out.println("++++++ handle workload: " + heldExecutorMapping.get(operatorId));
		int newParallelism = executorMapping.keySet().size();
		OperatorWorkloadsAssignment operatorWorkloadsAssignment;
		if (newParallelism == heldWorkloadsAssignmentMap.get(operatorId).getNumOpenedSubtask()) {
			operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(
				executorMapping,
				heldExecutorMapping.get(operatorId),
				heldWorkloadsAssignmentMap.get(operatorId),
				newParallelism);
		} else if (newParallelism > heldWorkloadsAssignmentMap.get(operatorId).getNumOpenedSubtask()) {
			operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(
				executorMapping,
				heldExecutorMapping.get(operatorId),
				heldWorkloadsAssignmentMap.get(operatorId),
				newParallelism);
		} else {
			operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(
				executorMapping,
				heldExecutorMapping.get(operatorId),
				heldWorkloadsAssignmentMap.get(operatorId),
				heldWorkloadsAssignmentMap.get(operatorId).getNumOpenedSubtask());
		}

		heldWorkloadsAssignmentMap.put(operatorId, operatorWorkloadsAssignment);
		heldExecutorMapping.put(operatorId, executorMapping);
		System.out.println("++++++after handle workloads " + heldExecutorMapping.get(operatorId));
		return operatorWorkloadsAssignment;
	}

	public OperatorWorkloadsAssignment getHeldOperatorWorkloadsAssignment(int operatorId) {
		return heldWorkloadsAssignmentMap.get(operatorId);
	}
}

package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WorkloadsAssignmentHandler {
	// operator -> workload assignment
	private Map<Integer, OperatorWorkloadsAssignment> heldWorkloadsAssignmentMap;

	public WorkloadsAssignmentHandler(StreamJobExecutionPlan jobExecutionPlan) {
		heldWorkloadsAssignmentMap = setupWorkloadsAssignmentMapFromExecutionPlan(jobExecutionPlan);
	}

	public Map<Integer, OperatorWorkloadsAssignment> setupWorkloadsAssignmentMapFromExecutionPlan(StreamJobExecutionPlan heldExecutionPlan) {
		Map<Integer, OperatorWorkloadsAssignment> heldWorkloadsAssignmentMap = new HashMap<>();
		for (Iterator<OperatorDescriptor> it = heldExecutionPlan.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			int operatorID = descriptor.getOperatorID();
			int parallelism = descriptor.getParallelism();
			Map<Integer, List<Integer>> keyStateAllocation = descriptor.getKeyStateAllocation();
			OperatorWorkloadsAssignment operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(keyStateAllocation, parallelism);
			heldWorkloadsAssignmentMap.put(operatorID, operatorWorkloadsAssignment);
		}
		return heldWorkloadsAssignmentMap;
	}

	public OperatorWorkloadsAssignment handleWorkloadsReallocate(int operatorId, Map<Integer, List<Integer>> executorMapping) {
		int newParallelism = executorMapping.keySet().size();
		OperatorWorkloadsAssignment operatorWorkloadsAssignment;
		if (newParallelism >= heldWorkloadsAssignmentMap.get(operatorId).getNumOpenedSubtask()) {
			operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(
				executorMapping,
				heldWorkloadsAssignmentMap.get(operatorId).getPartitionAssignment(),
				heldWorkloadsAssignmentMap.get(operatorId),
				newParallelism);
			heldWorkloadsAssignmentMap.put(operatorId, operatorWorkloadsAssignment);
		} else {
			operatorWorkloadsAssignment = new OperatorWorkloadsAssignment(
				executorMapping,
				heldWorkloadsAssignmentMap.get(operatorId).getPartitionAssignment(),
				heldWorkloadsAssignmentMap.get(operatorId),
				heldWorkloadsAssignmentMap.get(operatorId).getNumOpenedSubtask());
			heldWorkloadsAssignmentMap.put(operatorId, operatorWorkloadsAssignment);
		}
		return operatorWorkloadsAssignment;
	}

	public OperatorWorkloadsAssignment getHeldOperatorWorkloadsAssignment(int operatorId) {
		return heldWorkloadsAssignmentMap.get(operatorId);
	}
}

package org.apache.flink.streaming.controlplane.streammanager.insts;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public interface DeployGraphState {
	/**
	 * Get all hosts of current job
	 *
	 * @return
	 */
	Host[] getHosts();

	/**
	 * Get one of the parallel task of one operator.
	 *
	 * @param operatorID the operator id of this operator
	 * @param offset     represent which parallel instance of this operator
	 * @return
	 */
	OperatorTask getTask(OperatorID operatorID, int offset);

	class OperatorTask {
		int ownThreads;
		Host location;
		private LogicalSlot slot;

		public OperatorTask(LogicalSlot slot, Host host) {
			this.slot = slot;
			this.location = Preconditions.checkNotNull(host);
			this.ownThreads = slot.getPhysicalSlotNumber();
			host.addContainedTask(this);
		}
	}

	class Host {
		int numCpus;
		/* in bytes */
		int memory;
		List<OperatorTask> containedTasks;

		public Host(TaskManagerLocation taskManagerLocation) {
			containedTasks = new ArrayList<>();
		}

		public void addContainedTask(OperatorTask operatorTask){
			containedTasks.add(operatorTask);
		}
	}
}

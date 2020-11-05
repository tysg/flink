package org.apache.flink.streaming.controlplane.streammanager.insts;

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.List;

public interface DeployGraphState {
	/**
	 * Get all hosts of current job
	 *
	 * @return
	 */
	List<Host> getHosts();

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
	}

	class Host {
		int numCpus;
		/* in bytes */
		int memory;
		List<OperatorTask> containedTasks;
	}
}

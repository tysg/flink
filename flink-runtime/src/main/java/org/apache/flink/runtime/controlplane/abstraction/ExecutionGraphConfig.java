package org.apache.flink.runtime.controlplane.abstraction;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Deployment state should be get in real time since it is changed due to streaming system fail/restart strategy
 */
public interface ExecutionGraphConfig {
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
	OperatorTask getTask(Integer operatorID, int offset);

	class OperatorTask {
		// task own threads
		int ownThreads;
		Host location;

		public OperatorTask(int ownThreads, Host location) {
			this.ownThreads = ownThreads;
			this.location = location;
			location.addContainedTask(this);
		}
	}

	class Host {
		// host network address
		InetAddress address;
		// host number of cpus
		int numCpus;
		/* host memory in bytes */
		int memory;

		List<OperatorTask> containedTasks;

		public Host(InetAddress address, int numCpus, int memory) {
			this.address = address;
			this.numCpus = numCpus;
			this.memory = memory;
			containedTasks = new ArrayList<>();
		}

		void addContainedTask(OperatorTask operatorTask){
			containedTasks.add(operatorTask);
		}
	}
}

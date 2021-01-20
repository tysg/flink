package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * The user defined model implement some control policy, it first need to implement this interface
 */
public interface ControlPolicy {

	void startControllers();

	void stopControllers();

	/**
	 * How stream manager notify one User Defined Model that one state update is completed?
	 */
	void onChangeCompleted(Throwable throwable);
}

package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public abstract class TaskOperatorManager {

	private final AbstractInvokable invokable;

	public TaskOperatorManager(AbstractInvokable invokable){
		this.invokable = invokable;
	}

	/**
	 * Substitute the operator here
	 * @param updatedConfig
	 * @param operatorID
	 */
	public final void update(Configuration updatedConfig, OperatorID operatorID){
		System.out.println("Now it's time for task config updater");
		try {
			invokable.updateOperator(updatedConfig, operatorID);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

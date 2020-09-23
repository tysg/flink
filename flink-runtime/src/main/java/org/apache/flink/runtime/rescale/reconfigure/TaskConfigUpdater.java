package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.Task;

public abstract class TaskConfigUpdater {
	private final Task task;

	public TaskConfigUpdater(Task task){
		this.task = task;
	}

	public void update(Configuration updatedConfig, OperatorID operatorID){
		System.out.println("Now it's time for task config updater");
	}
}

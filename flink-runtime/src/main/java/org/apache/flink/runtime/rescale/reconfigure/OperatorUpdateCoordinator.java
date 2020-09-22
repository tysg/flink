package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;


public class OperatorUpdateCoordinator {

	private JobGraph jobGraph;
	private ExecutionGraph executionGraph;
	private ClassLoader userCodeClassLoader;

	public OperatorUpdateCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;
	}

	public void triggerUpdate(JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID){
		System.out.println("some one want to triggerOperatorUpdate using OperatorUpdateCoordinator?");

	}
}

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
		this.userCodeClassLoader = executionGraph.getUserClassLoader();
	}

	public void triggerUpdate(JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID){
		System.out.println("some one want to triggerOperatorUpdate using OperatorUpdateCoordinator?");
		// By evaluating:
		// "new StreamConfig((executionGraph.tasks.values().toArray()[1]).jobVertex.configuration).getTransitiveChainedTaskConfigs(userCodeClassLoader).get(4).getStreamOperatorFactory(userCodeClassLoader).getOperator()"
		// The logic in execution Graph has been modified since now they are in same process which sharing the same reference

		// some related to deploy here...

	}
}

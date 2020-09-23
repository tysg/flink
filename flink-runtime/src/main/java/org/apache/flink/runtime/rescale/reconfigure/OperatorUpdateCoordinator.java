package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;


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
		this.jobGraph = jobGraph;
		// By evaluating:
		// "new StreamConfig((executionGraph.tasks.values().toArray()[1]).jobVertex.configuration).getTransitiveChainedTaskConfigs(userCodeClassLoader).get(4).getStreamOperatorFactory(userCodeClassLoader).getOperator()"
		// The logic in execution Graph has been modified since now they are in same process which sharing the same reference

		// some deploy related here...
		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(targetVertexID);
		Preconditions.checkNotNull(executionJobVertex, "can not found this execution job vertex");

		ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(executionJobVertex.getTaskVertices().length);
		for(ExecutionVertex vertex: executionJobVertex.getTaskVertices()){
			Execution execution = vertex.getCurrentExecutionAttempt();
			futures.add(execution.scheduleOperatorUpdate(operatorID));
		}
		FutureUtils.completeAll(futures);
		System.out.println("Finished notify tasks to update operator");
	}

}

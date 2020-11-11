package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ReconfigureCoordinator extends AbstractCoordinator {

	public ReconfigureCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
		super(jobGraph, executionGraph);
	}

	@Override
	public CompletableFuture<Void> synchronizeTasks(List<Tuple2<Integer, Integer>> taskList) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> deployTasks(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> cancelTasks(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> updateMapping(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> updateState(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Acknowledge> updateFunction(int vertexID, int offset) {
		System.out.println("some one want to triggerOperatorUpdate?");
		OperatorID operatorID = super.operatorIDMap.get(vertexID);
		JobVertexID jobVertexID = JobVertexID.fromHexString(operatorID.toHexString());

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "can not found this execution job vertex");

		ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(executionJobVertex.getTaskVertices().length);
		for (ExecutionVertex vertex : executionJobVertex.getTaskVertices()) {
			Execution execution = vertex.getCurrentExecutionAttempt();
			futures.add(execution.scheduleOperatorUpdate(operatorID));
		}
		return FutureUtils.completeAll(futures).thenApply(o -> Acknowledge.get());
	}
}

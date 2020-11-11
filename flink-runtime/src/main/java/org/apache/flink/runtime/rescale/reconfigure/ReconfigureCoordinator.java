package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;

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
	public CompletableFuture<Acknowledge> updateFunction(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}
}

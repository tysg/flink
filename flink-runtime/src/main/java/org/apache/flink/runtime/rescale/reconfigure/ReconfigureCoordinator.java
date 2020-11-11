package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rescale.RescalepointAcknowledgeListener;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ReconfigureCoordinator extends AbstractCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(ReconfigureCoordinator.class);

	public ReconfigureCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
		super(jobGraph, executionGraph);
	}

	/**
	 * Synchronize the tasks in the list by inject barrier from source operator.
	 * When those tasks received barrier, they should stop processing and wait next instruction.
	 * <p>
	 * To pause processing, one solution is using the pause controller to stop reading message on the mailProcessor
	 * Another solution is to substitute the input channel.
	 * <p>
	 * Todo how and when to resume the operator processing? Resumed by special primitiveOperation?
	 *
	 * @param taskList The list of task id, each id is a tuple which the first element is operator id and the second element is offset
	 * @return A future that representing  synchronize is successful
	 */
	@Override
	public CompletableFuture<Void> synchronizeTasks(List<Tuple2<Integer, Integer>> taskList) {
		// we should first check if the tasks is stateless
		// if stateless, do not need to synchronize,

		// stateful tasks, inject barrier
		List<Tuple2<JobVertexID, Integer>> vertexIDList = taskList.stream()
			.map(t -> Tuple2.of(JobVertexID.fromHexString(operatorIDMap.get(t.f0).toHexString()), t.f1))
			.collect(Collectors.toList());
		SynchronizeOperation syncOp = new SynchronizeOperation(vertexIDList);
		CompletableFuture<Map<OperatorID, OperatorState>> collectedOperatorStateFuture = syncOp.sync();

		return collectedOperatorStateFuture.thenAccept(state -> LOG.debug("synchronizeTasks successful"));
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

		if (offset >= 0) {
			Preconditions.checkArgument(offset < executionJobVertex.getParallelism(), "offset out of boundary");
			ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[offset];
			return executionVertex.getCurrentExecutionAttempt()
				.scheduleOperatorUpdate(operatorID)
				.thenApply(o -> Acknowledge.get());
		}

		ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(executionJobVertex.getTaskVertices().length);
		for (ExecutionVertex vertex : executionJobVertex.getTaskVertices()) {
			Execution execution = vertex.getCurrentExecutionAttempt();
			futures.add(execution.scheduleOperatorUpdate(operatorID));
		}
		return FutureUtils.completeAll(futures).thenApply(o -> Acknowledge.get());
	}

	// temporary use RescalepointAcknowledgeListener
	private class SynchronizeOperation implements RescalepointAcknowledgeListener {

		private final List<ExecutionAttemptID> notYetAcknowledgedTasks = new LinkedList<>();

		private final List<Tuple2<JobVertexID, Integer>> vertexIdList;
		private final CompletableFuture<Map<OperatorID, OperatorState>> finishedFuture;

		private long checkpointId;
		private final Object lock = new Object();

		SynchronizeOperation(List<Tuple2<JobVertexID, Integer>> vertexIdList) {
			this.vertexIdList = vertexIdList;
			finishedFuture = new CompletableFuture<>();
		}

		private CompletableFuture<Map<OperatorID, OperatorState>> sync() {
			CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

			checkNotNull(checkpointCoordinator, "do not have checkpointCoordinator");
			checkpointCoordinator.setRescalepointAcknowledgeListener(this);

			// add needed acknowledge tasks
			for (Tuple2<JobVertexID, Integer> taskID : vertexIdList) {
				ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(taskID.f0);
				checkNotNull(executionJobVertex);
				int offset = taskID.f1;
				if (offset < 0) {
					for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
						notYetAcknowledgedTasks.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
					}
				} else {
					checkArgument(offset < executionJobVertex.getParallelism(), "offset out of boundary");
					notYetAcknowledgedTasks.add(executionJobVertex.getTaskVertices()[offset].getCurrentExecutionAttempt().getAttemptId());
				}
			}
			// make affected task prepare synchronization
			CompletableFuture<Void> rescaleCompleted = FutureUtils.completedVoidFuture()
				.thenCompose(
					(o1) -> {
						Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
						return FutureUtils
							.completeAll(rescaleCandidatesFutures);
					})
				.thenRunAsync(() -> {
					try {
						checkpointCoordinator.stopCheckpointScheduler();
						// temporary use rescale point
						checkpointCoordinator.triggerRescalePoint(System.currentTimeMillis());
					} catch (Exception e) {
						throw new CompletionException(e);
					}
				});
			return finishedFuture;
		}


		@Override
		public void onReceiveRescalepointAcknowledge(ExecutionAttemptID attemptID, PendingCheckpoint checkpoint) {
			if (checkpointId == checkpoint.getCheckpointId()) {

				CompletableFuture.runAsync(() -> {
					LOG.info("++++++ Received Rescalepoint Acknowledgement");
					try {
						synchronized (lock) {
							if (notYetAcknowledgedTasks.isEmpty()) {
								// late come in snapshot, ignore it
								return;
							}
							notYetAcknowledgedTasks.remove(attemptID);

							if (notYetAcknowledgedTasks.isEmpty()) {
								LOG.info("++++++ handle operator states");
								finishedFuture.complete(new HashMap<>(checkpoint.getOperatorStates()));
							}
						}
					} catch (Exception e) {
						throw new CompletionException(e);
					}
				});
			}
		}

		@Override
		public void setCheckpointId(long checkpointId) {
			this.checkpointId = checkpointId;
		}
	}
}

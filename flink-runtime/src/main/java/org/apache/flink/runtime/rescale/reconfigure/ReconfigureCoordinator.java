package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rescale.RescaleID;
import org.apache.flink.runtime.rescale.RescaleOptions;
import org.apache.flink.runtime.rescale.RescalepointAcknowledgeListener;
import org.apache.flink.runtime.state.KeyGroupRange;
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
	private SynchronizeOperation currentSyncOp = null;

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
	public CompletableFuture<Void> synchronizePauseTasks(List<Tuple2<Integer, Integer>> taskList) {
		// we should first check if the tasks is stateless
		// if stateless, do not need to synchronize,
		System.out.println("start synchronizing..."+taskList);
		// stateful tasks, inject barrier
		List<Tuple2<JobVertexID, Integer>> vertexIDList = taskList.stream()
			.map(t -> Tuple2.of(rawVertexIDToJobVertexID(t.f0), t.f1))
			.collect(Collectors.toList());
		SynchronizeOperation syncOp = new SynchronizeOperation(vertexIDList);
		try {
			CompletableFuture<Map<OperatorID, OperatorState>> collectedOperatorStateFuture = syncOp.sync();
			// some check related here
			currentSyncOp = syncOp;
			return collectedOperatorStateFuture.thenAccept(state ->
			{
				System.out.println("synchronizeTasks successful");
				LOG.debug("synchronizeTasks successful");
			});
		}catch (Exception e){
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * Resume the paused tasks by synchronize.
	 * <p>
	 * In implementation, since we use MailBoxProcessor to pause tasks,
	 * to make this resume methods make sense, the task's MailBoxProcessor should not be changed.
	 *
	 * @param taskList The list of task id, each id is a tuple which the first element is operator id and the second element is offset
	 * @return
	 */
	@Override
	public CompletableFuture<Void> resumeTasks(List<Tuple2<Integer, Integer>> taskList) {

		System.out.println("resuming... "+taskList);
		List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
		for (Tuple2<Integer, Integer> taskID : taskList) {
			JobVertexID vertexID = rawVertexIDToJobVertexID(taskID.f0);
			ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(vertexID);
			checkNotNull(executionJobVertex);
			int offset = taskID.f1;
			if (offset < 0) {
				for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
					affectedExecutionPrepareSyncFutures.add(
						executionVertex.getCurrentExecutionAttempt().scheduleForInterTaskSync(TaskOperatorManager.NEED_RESUME_REQUEST));
				}
			} else {
				checkArgument(offset < executionJobVertex.getParallelism(), "offset out of boundary");
				Execution targetExecution = executionJobVertex.getTaskVertices()[offset].getCurrentExecutionAttempt();
				affectedExecutionPrepareSyncFutures.add(targetExecution.scheduleForInterTaskSync(TaskOperatorManager.NEED_RESUME_REQUEST));
			}
		}
		// make affected task resume
		return FutureUtils.completeAll(affectedExecutionPrepareSyncFutures);
	}

	@Override
	public CompletableFuture<Void> deployTasks(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> cancelTasks(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * update result partition on this operator,
	 * update input gates, key group range on its down stream operator
	 *
	 * @param srcOpID  the operator id of this operator
	 * @param destOpID represent which parallel instance of this operator, -1 means all parallel instance
	 * @return
	 */
	@Override
	public CompletableFuture<Void> updateMapping(int srcOpID, int destOpID) {
		System.out.println("update mapping...");
		final RescaleID rescaleID = currentSyncOp.rescaleID;
		final List<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
		try {
			// update result partition
			JobVertexID srcJobVertexID = rawVertexIDToJobVertexID(srcOpID);
			ExecutionJobVertex srcJobVertex = executionGraph.getJobVertex(srcJobVertexID);
			Preconditions.checkNotNull(srcJobVertex, "can not found this execution job vertex");
			srcJobVertex.cleanBeforeRescale();
			for (ExecutionVertex vertex : srcJobVertex.getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				execution.updateProducedPartitions(rescaleID);
				rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleID, RescaleOptions.RESCALE_PARTITIONS_ONLY, null));
			}
			// update input gates
			JobVertexID destJobVertexID = rawVertexIDToJobVertexID(destOpID);
			ExecutionJobVertex destJobVertex = executionGraph.getJobVertex(destJobVertexID);
			Preconditions.checkNotNull(destJobVertex, "can not found this execution job vertex");

			RemappingAssignment remappingAssignment = new RemappingAssignment(
				heldExecutionPlan.getKeyMapping(srcOpID).get(destOpID)
			);
			for (int i = 0; i < destJobVertex.getParallelism(); i++) {
				ExecutionVertex vertex = destJobVertex.getTaskVertices()[i];
				Execution execution = vertex.getCurrentExecutionAttempt();
				rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleID, RescaleOptions.RESCALE_GATES_ONLY, null));
				rescaleCandidatesFutures.add(execution.scheduleRescale(
					rescaleID,
					RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
					remappingAssignment.getAlignedKeyGroupRange(i)));
			}
		} catch (ExecutionGraphException e) {
			return FutureUtils.completedExceptionally(e);
		}
		return FutureUtils.completeAll(rescaleCandidatesFutures);
	}

	/**
	 * update the key state in destination operator
	 *
	 * @param keySenderID the id of which operator send keys to destination operator
	 * @param operatorID  the id of operator that need to update state
	 * @param offset      the sub-operator offset of update stated needed operator
	 * @return
	 */
	@Override
	public CompletableFuture<Void> updateState(int keySenderID, int operatorID, int offset) {
		System.out.println("update state...");
		final RescaleID rescaleID = currentSyncOp.rescaleID;
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "can not found this execution job vertex");

		RemappingAssignment remappingAssignment = new RemappingAssignment(
			heldExecutionPlan.getKeyMapping(keySenderID).get(operatorID)
		);
		CompletableFuture<Void> assignStateFuture = currentSyncOp.finishedFuture.thenAccept(
			state -> {
				StateAssignmentOperation stateAssignmentOperation =
					new StateAssignmentOperation(currentSyncOp.checkpointId, Collections.singleton(executionJobVertex), state, true);
				stateAssignmentOperation.setForceRescale(true);
				// think about latter
				stateAssignmentOperation.setRedistributeStrategy(remappingAssignment);

				LOG.info("++++++ start to assign states");
				stateAssignmentOperation.assignStates();
			}
		);
		return assignStateFuture.thenCompose(
			o -> {
				final List<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
				try {
					if (offset >= 0) {
						Preconditions.checkArgument(offset < executionJobVertex.getParallelism(), "offset out of boundary");
						ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[offset];
						rescaleCandidatesFutures.add(
							executionVertex.getCurrentExecutionAttempt().scheduleRescale(
								rescaleID,
								RescaleOptions.RESCALE_REDISTRIBUTE,
								remappingAssignment.getAlignedKeyGroupRange(offset)
							));
					} else {
						for (int i = 0; i < executionJobVertex.getParallelism(); i++) {
							ExecutionVertex vertex = executionJobVertex.getTaskVertices()[i];
							Execution execution = vertex.getCurrentExecutionAttempt();
							rescaleCandidatesFutures.add(execution.scheduleRescale(
								rescaleID,
								RescaleOptions.RESCALE_REDISTRIBUTE,
								remappingAssignment.getAlignedKeyGroupRange(i)
							));
						}
					}
				} catch (ExecutionGraphException e) {
					e.printStackTrace();
				}
				return FutureUtils.completeAll(rescaleCandidatesFutures);
			}
		);
	}

	/**
	 * This method also will wake up the paused tasks.
	 *
	 * @param vertexID the operator id of this operator
	 * @param offset   represent which parallel instance of this operator, -1 means all parallel instance
	 * @return
	 */
	@Override
	public CompletableFuture<Acknowledge> updateFunction(int vertexID, int offset) {
		System.out.println("some one want to triggerOperatorUpdate?");
		OperatorID operatorID = super.operatorIDMap.get(vertexID);
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(vertexID);

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

		private final Set<ExecutionAttemptID> notYetAcknowledgedTasks = new HashSet<>();

		private final List<Tuple2<JobVertexID, Integer>> vertexIdList;
		private final Object lock = new Object();
		private final RescaleID rescaleID = RescaleID.generateNextID();

		private final CompletableFuture<Map<OperatorID, OperatorState>> finishedFuture;

		private long checkpointId;

		SynchronizeOperation(List<Tuple2<JobVertexID, Integer>> vertexIdList) {
			this.vertexIdList = vertexIdList;
			finishedFuture = new CompletableFuture<>();
		}

		private CompletableFuture<Map<OperatorID, OperatorState>> sync() {

			// add needed acknowledge tasks
			List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
			for (Tuple2<JobVertexID, Integer> taskID : vertexIdList) {
				ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(taskID.f0);
				checkNotNull(executionJobVertex);
				int offset = taskID.f1;
				if (offset < 0) {
					for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
						affectedExecutionPrepareSyncFutures.add(
							executionVertex.getCurrentExecutionAttempt().scheduleForInterTaskSync(TaskOperatorManager.NEED_SYNC_REQUEST));
						notYetAcknowledgedTasks.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
					}
				} else {
					checkArgument(offset < executionJobVertex.getParallelism(), "offset out of boundary");
					Execution targetExecution = executionJobVertex.getTaskVertices()[offset].getCurrentExecutionAttempt();
					affectedExecutionPrepareSyncFutures.add(targetExecution.scheduleForInterTaskSync(TaskOperatorManager.NEED_SYNC_REQUEST));
					notYetAcknowledgedTasks.add(targetExecution.getAttemptId());
				}
			}
			// make affected task prepare synchronization
			FutureUtils.completeAll(affectedExecutionPrepareSyncFutures)
				.exceptionally(throwable -> {
					throwable.printStackTrace();
					return null;
				})
				.thenRunAsync(() -> {
					try {
						CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
						checkNotNull(checkpointCoordinator, "do not have checkpointCoordinator");
						checkpointCoordinator.stopCheckpointScheduler();
						checkpointCoordinator.setRescalepointAcknowledgeListener(this);
						// temporary use rescale point
						System.out.println("send barrier...");
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

								CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
								checkNotNull(checkpointCoordinator);
								if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
									checkpointCoordinator.startCheckpointScheduler();
								}

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
			System.out.println("trigger rescale point with check point id:" + checkpointId);
		}
	}

	public static class RemappingAssignment {

		private final Map<Integer, List<Integer>> keymapping;
		private final List<KeyGroupRange> alignedKeyGroupRanges;

		RemappingAssignment(List<List<Integer>> mapping) {
			keymapping = new HashMap<>();
			for (int i = 0; i < mapping.size(); i++) {
				keymapping.put(i, mapping.get(i));
			}
			alignedKeyGroupRanges = generateAlignedKeyGroupRanges(mapping);
		}

		private List<KeyGroupRange> generateAlignedKeyGroupRanges(List<List<Integer>> partitionAssignment) {
			int keyGroupStart = 0;
			List<KeyGroupRange> alignedKeyGroupRanges = new ArrayList<>();
			for (List<Integer> list : partitionAssignment) {
				int rangeSize = list.size();

				KeyGroupRange keyGroupRange = rangeSize == 0 ?
					KeyGroupRange.EMPTY_KEY_GROUP_RANGE :
					new KeyGroupRange(
						keyGroupStart,
						keyGroupStart + rangeSize - 1,
						list);

				alignedKeyGroupRanges.add(keyGroupRange);
				keyGroupStart += rangeSize;
			}
			return alignedKeyGroupRanges;
		}


		public Map<Integer, List<Integer>> getPartitionAssignment() {
			return keymapping;
		}

		public KeyGroupRange getAlignedKeyGroupRange(int subTaskIndex) {
			return alignedKeyGroupRanges.get(subTaskIndex);
		}
	}
}

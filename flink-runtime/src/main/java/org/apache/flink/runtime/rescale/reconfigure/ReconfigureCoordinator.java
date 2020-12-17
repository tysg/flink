package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
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
	 * To pause processing, one solution is using the pause controller
	 * For non-source operator tasks, we use pause controller to stop reading message on the mailProcessor.
	 * <p>
	 * Regard as source stream tasks, since it does not have input, we will recreate its partitions with new configuration,
	 * thus its children stream task will temporarily not receive input from this source task until they update their input gates.
	 * Here we assume that the prepare api has been called before, so the execution graph already has the latest configuration.
	 * This is to prevent that down stream task received data that does not belongs to their key set.
	 * <p>
	 *
	 * @param taskList The list of task id, each id is a tuple which the first element is operator id and the second element is offset
	 * @return A future that representing  synchronize is successful
	 */
	@Override
	public CompletableFuture<Void> synchronizePauseTasks(List<Tuple2<Integer, Integer>> taskList) {
		// we should first check if the tasks is stateless
		// if stateless, do not need to synchronize,
		System.out.println("start synchronizing..." + taskList);
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
		} catch (Exception e) {
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
		System.out.println("resuming... " + taskList);
		checkNotNull(currentSyncOp, "have you call sync op before?");
		return currentSyncOp.resumeTasks(taskList).thenAccept(o -> currentSyncOp = null);
	}

	@Override
	public CompletableFuture<Void> deployTasks(int operatorID, int oldParallelism) {
		System.out.println("deploying... tasks of " + operatorID);
		JobVertexID srcJobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex srcJobVertex = executionGraph.getJobVertex(srcJobVertexID);
		Preconditions.checkNotNull(srcJobVertex, "can not found this execution job vertex");
		srcJobVertex.cleanBeforeRescale();

		Collection<CompletableFuture<Execution>> allocateSlotFutures =
			new ArrayList<>(srcJobVertex.getParallelism() - oldParallelism);

		ExecutionVertex[] taskVertices = srcJobVertex.getTaskVertices();
		RescaleID rescaleID = RescaleID.generateNextID();
		for (int i = oldParallelism; i < srcJobVertex.getParallelism(); i++) {
			Execution executionAttempt = taskVertices[i].getCurrentExecutionAttempt();
			allocateSlotFutures.add(executionAttempt.allocateAndAssignSlotForExecution(rescaleID));
		}

		return FutureUtils.combineAll(allocateSlotFutures)
			.whenComplete((executions, throwable) -> {
					if (throwable != null) {
						throwable.printStackTrace();
						throw new CompletionException(throwable);
					}
					System.out.println("allocated resource for new tasks of " + operatorID);
				}
			).thenCompose(executions -> {
					Collection<CompletableFuture<Acknowledge>> deployFutures =
						new ArrayList<>(srcJobVertex.getParallelism() - oldParallelism);
					for (Execution execution : executions) {
						try {
							deployFutures.add(execution.deploy());
						} catch (JobException e) {
							e.printStackTrace();
						}
					}
					return FutureUtils.waitForAll(deployFutures);
				}
			).thenCompose(executions -> {
				try {
					final List<CompletableFuture<Void>> finishedFutureList = new ArrayList<>();
					updatePartitionAndDownStreamGates(operatorID, rescaleID, finishedFutureList);
					return FutureUtils.waitForAll(finishedFutureList);
				} catch (ExecutionGraphException e) {
					e.printStackTrace();
					return FutureUtils.completedExceptionally(e);
				}
			});
	}

	@Override
	public CompletableFuture<Void> cancelTasks(int operatorID, int offset) {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * update result partition on this operator,
	 * update input gates, key group range on its down stream operator
	 *
	 * @return
	 */
	@Override
	public CompletableFuture<Void> updateUpstreamKeyMapping(int destOpID) {
		System.out.println("update mapping...");
		final List<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
		try {
			RescaleID rescaleID = RescaleID.generateNextID();
			for(OperatorDescriptor src: heldExecutionPlan.getOperatorDescriptorByID(destOpID).getParents()){
				// todo some partitions may not need modified, for example, broad cast partitioner
				updatePartitionAndDownStreamGates(src.getOperatorID(), rescaleID, rescaleCandidatesFutures);
			}
			// update key group range in target stream
			JobVertexID destJobVertexID = rawVertexIDToJobVertexID(destOpID);
			ExecutionJobVertex destJobVertex = executionGraph.getJobVertex(destJobVertexID);
			Preconditions.checkNotNull(destJobVertex, "can not found this execution job vertex");
			RemappingAssignment remappingAssignment = new RemappingAssignment(
				heldExecutionPlan.getKeyStateAllocation(destOpID)
			);
			for (int i = 0; i < destJobVertex.getParallelism(); i++) {
				ExecutionVertex vertex = destJobVertex.getTaskVertices()[i];
				Execution execution = vertex.getCurrentExecutionAttempt();
				if (execution != null && execution.getState() == ExecutionState.RUNNING) {
					rescaleCandidatesFutures.add(execution.scheduleRescale(
						null,
						RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
						remappingAssignment.getAlignedKeyGroupRange(i)));
				} else {
					vertex.assignKeyGroupRange(remappingAssignment.getAlignedKeyGroupRange(i));
				}
			}
		} catch (ExecutionGraphException e) {
			return FutureUtils.completedExceptionally(e);
		}
		return FutureUtils.completeAll(rescaleCandidatesFutures);
	}

	private void updatePartitionAndDownStreamGates(int srcOpID, RescaleID rescaleID, List<CompletableFuture<Void>> rescaleCandidatesFutures) throws ExecutionGraphException {
		// update result partition
		JobVertexID srcJobVertexID = rawVertexIDToJobVertexID(srcOpID);
		ExecutionJobVertex srcJobVertex = executionGraph.getJobVertex(srcJobVertexID);
		Preconditions.checkNotNull(srcJobVertex, "can not found this execution job vertex");
		srcJobVertex.cleanBeforeRescale();
		for (ExecutionVertex vertex : srcJobVertex.getTaskVertices()) {
			Execution execution = vertex.getCurrentExecutionAttempt();
			if (execution != null && execution.getState() == ExecutionState.RUNNING) {
				execution.updateProducedPartitions(rescaleID);
				rescaleCandidatesFutures.add(execution.scheduleRescale(null, RescaleOptions.RESCALE_PARTITIONS_ONLY, null));
			}
		}
		// update input gates in child stream of source op
		for (OperatorDescriptor child : heldExecutionPlan.getOperatorDescriptorByID(srcOpID).getChildren()) {
			JobVertexID childID = rawVertexIDToJobVertexID(child.getOperatorID());
			updateGates(childID, rescaleCandidatesFutures);
		}
	}

	private void updateGates(JobVertexID jobVertexID, List<CompletableFuture<Void>> futureList) throws ExecutionGraphException {
		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(jobVertex, "can not found this execution job vertex");
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			Execution execution = vertex.getCurrentExecutionAttempt();
			if (execution != null && execution.getState() == ExecutionState.RUNNING) {
				futureList.add(execution.scheduleRescale(null, RescaleOptions.RESCALE_GATES_ONLY, null));
			}
		}
	}

	/**
	 * update the key state in destination operator
	 *
	 * @param operatorID  the id of operator that need to update state
	 * @param offset      the sub-operator offset of update stated needed operator
	 * @return
	 */
	@Override
	public CompletableFuture<Void> updateState(int operatorID, int offset) {
		System.out.println("update state...");
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "can not found this execution job vertex");

		RemappingAssignment remappingAssignment = new RemappingAssignment(
			heldExecutionPlan.getKeyStateAllocation(operatorID)
		);
		checkNotNull(currentSyncOp, "no state collected currently, have you synchronized first?");
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
		return assignStateFuture.thenCompose(o -> {
				List<ExecutionVertex> operatedVertex = getOperatedVertex(executionJobVertex, offset);
				final List<CompletableFuture<?>> rescaleCandidatesFutures =
					operatedVertex.stream()
						.map(ExecutionVertex::getCurrentExecutionAttempt)
						.filter(execution -> execution != null && execution.getState() == ExecutionState.RUNNING)
						.map(execution -> {
								try {
									return execution.scheduleRescale(
										null,
										RescaleOptions.RESCALE_STATE_ONLY,
										remappingAssignment.getAlignedKeyGroupRange(execution.getParallelSubtaskIndex())
									);
								} catch (Exception e) {
									e.printStackTrace();
									return FutureUtils.completedExceptionally(e);
								}
							}
						).collect(Collectors.toList());
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

		List<ExecutionVertex> operatedVertex = getOperatedVertex(executionJobVertex, offset);
		final List<CompletableFuture<?>> resultFutures =
			operatedVertex.stream()
				.map(ExecutionVertex::getCurrentExecutionAttempt)
				.filter(execution -> execution != null && execution.getState() == ExecutionState.RUNNING)
				.map(execution -> {
						try {
							return execution.scheduleOperatorUpdate(operatorID);
						} catch (Exception e) {
							e.printStackTrace();
							return FutureUtils.completedExceptionally(e);
						}
					}
				).collect(Collectors.toList());
		return FutureUtils.completeAll(resultFutures).thenApply(o -> Acknowledge.get());
	}

	private static List<ExecutionVertex> getOperatedVertex(ExecutionJobVertex executionJobVertex, int offset) {
		List<ExecutionVertex> executionVertices = new ArrayList<>();
		if (offset < 0) {
			executionVertices.addAll(Arrays.asList(executionJobVertex.getTaskVertices()));
		} else {
			checkArgument(offset < executionJobVertex.getParallelism(), "offset out of boundary");
			executionVertices.add(executionJobVertex.getTaskVertices()[offset]);
		}
		return executionVertices;
	}

	// temporary use RescalepointAcknowledgeListener
	private class SynchronizeOperation implements RescalepointAcknowledgeListener {

		private final Set<ExecutionAttemptID> notYetAcknowledgedTasks = new HashSet<>();

		private final List<Tuple2<JobVertexID, Integer>> vertexIdList;
		private final Object lock = new Object();

		private final CompletableFuture<Map<OperatorID, OperatorState>> finishedFuture;
		final RescaleID rescaleID = RescaleID.generateNextID();

		private long checkpointId;

		SynchronizeOperation(List<Tuple2<JobVertexID, Integer>> vertexIdList) {
			this.vertexIdList = vertexIdList;
			finishedFuture = new CompletableFuture<>();
		}

		private CompletableFuture<Map<OperatorID, OperatorState>> sync() throws ExecutionGraphException {
			// add needed acknowledge tasks
			List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
			for (Tuple2<JobVertexID, Integer> taskID : vertexIdList) {
				ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(taskID.f0);
				checkNotNull(executionJobVertex);
				List<ExecutionVertex> operatedVertex = getOperatedVertex(executionJobVertex, taskID.f1);
				if (executionJobVertex.getInputs().isEmpty()) {
					// this is source task vertex
					affectedExecutionPrepareSyncFutures.add(pauseSourceStreamTask(executionJobVertex, operatedVertex));
				} else {
					operatedVertex.stream()
						.map(ExecutionVertex::getCurrentExecutionAttempt)
						.filter(execution -> execution != null && execution.getState() == ExecutionState.RUNNING)
						.forEach(execution -> {
								affectedExecutionPrepareSyncFutures.add(execution.scheduleForInterTaskSync(TaskOperatorManager.NEED_SYNC_REQUEST));
								notYetAcknowledgedTasks.add(execution.getAttemptId());
							}
						);
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

		private CompletableFuture<Void> pauseSourceStreamTask(
			ExecutionJobVertex executionJobVertex,
			List<ExecutionVertex> operatedVertex) throws ExecutionGraphException {

			List<CompletableFuture<Void>> futureList = new ArrayList<>();
			executionJobVertex.cleanBeforeRescale();
			for (ExecutionVertex executionVertex : operatedVertex) {
				Execution execution = executionVertex.getCurrentExecutionAttempt();
				if (execution != null && execution.getState() == ExecutionState.RUNNING) {
					execution.updateProducedPartitions(rescaleID);
					futureList.add(execution.scheduleRescale(null, RescaleOptions.PREPARE_ONLY, null));
					notYetAcknowledgedTasks.add(execution.getAttemptId());
				}
			}
			return FutureUtils.completeAll(futureList);
		}

		private CompletableFuture<Void> resumeSourceStreamTask(
			int rawSourceVertexId,
			List<ExecutionVertex> operatedVertex) throws ExecutionGraphException {

			List<CompletableFuture<Void>> futureList = new ArrayList<>();
			for (ExecutionVertex executionVertex : operatedVertex) {
				// means has been resumed before
				if (!executionVertex.getRescaleId().equals(rescaleID)) {
					// all execution vertex of same job vertex has the same rescale id
					return FutureUtils.completedVoidFuture();
				}
			}
			for (OperatorDescriptor child : heldExecutionPlan.getOperatorDescriptorByID(rawSourceVertexId).getChildren()) {
				JobVertexID childID = rawVertexIDToJobVertexID(child.getOperatorID());
				try {
					updateGates(childID, futureList);
				} catch (ExecutionGraphException e) {
					e.printStackTrace();
				}
			}
			return FutureUtils.completeAll(futureList);
		}

		private CompletableFuture<Void> resumeTasks(List<Tuple2<Integer, Integer>> taskList) {
			List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
			for (Tuple2<Integer, Integer> taskID : taskList) {
				JobVertexID jobVertexID = rawVertexIDToJobVertexID(taskID.f0);
				ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
				checkNotNull(executionJobVertex);
				List<ExecutionVertex> operatedVertex = getOperatedVertex(executionJobVertex, taskID.f1);

				if (executionJobVertex.getInputs().isEmpty()) {
					// resume source stream task
					// update input gates in child stream of source op
					try {
						affectedExecutionPrepareSyncFutures.add(resumeSourceStreamTask(taskID.f0, operatedVertex));
					} catch (ExecutionGraphException e) {
						e.printStackTrace();
					}
				} else {
					operatedVertex.stream()
						.map(ExecutionVertex::getCurrentExecutionAttempt)
						.filter(execution -> execution != null && execution.getState() == ExecutionState.RUNNING)
						.forEach(execution ->
							affectedExecutionPrepareSyncFutures.add(execution.scheduleForInterTaskSync(TaskOperatorManager.NEED_RESUME_REQUEST))
						);
				}
			}
			// make affected task resume
			return FutureUtils.completeAll(affectedExecutionPrepareSyncFutures);
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

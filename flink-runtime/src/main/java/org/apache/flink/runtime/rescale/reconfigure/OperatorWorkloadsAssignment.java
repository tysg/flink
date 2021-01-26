package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class OperatorWorkloadsAssignment implements AbstractCoordinator.Diff {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorWorkloadsAssignment.class);

	public static final int UNUSED_SUBTASK = Integer.MAX_VALUE/2;

	private int numOpenedSubtask;

	private final OperatorWorkloadsAssignment oldRescalePA;

	// subtaskIndex -> partitions
	private Map<Integer, List<Integer>> partitionAssignment;

	// subtaskIndex (in flink) -> idInModel (in streamswitch)
	private Map<Integer, Integer> subtaskIndexMapping;

	// subtaskIndex (in flink) -> idInModel (in streamswitch, or said executorId)
	private final Map<Integer, Integer> executorIdMapping;

	private final List<KeyGroupRange> alignedKeyGroupRanges;

	private Map<Integer, Boolean> modifiedSubtaskMap;

	// this is used for remove the corresponding subtask
	private final Map<Integer, Boolean> removedSubtaskMap;

	private final boolean isScaling;

	public OperatorWorkloadsAssignment(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping,
		OperatorWorkloadsAssignment oldRescalePA,
		int numOpenedSubtask) {

		this.numOpenedSubtask = numOpenedSubtask;
		this.oldRescalePA = checkNotNull(oldRescalePA);

		checkState(checkPartitionAssignmentValidity(executorMapping),
			"executorMapping has null or empty partition");

		checkState(checkPartitionAssignmentValidity(oldExecutorMapping),
			"oldExecutorMapping has null or empty partition");

		this.partitionAssignment = new HashMap<>();
		this.subtaskIndexMapping = new HashMap<>();
		this.executorIdMapping = new HashMap<>();
		this.alignedKeyGroupRanges = new ArrayList<>();
		this.modifiedSubtaskMap = new HashMap<>();
		this.removedSubtaskMap = new HashMap<>();

		// here we copy and translate passed-in mapping
//		Map<Integer, List<Integer>> executorMapping = generateIntegerMap(executorMapping);
//		Map<Integer, List<Integer>> oldExecutorMapping = generateIntegerMap(executorMapping);

		int newParallelism = executorMapping.keySet().size();
		int oldParallelism = oldExecutorMapping.keySet().size();

		if (newParallelism > oldParallelism) {
			isScaling = true;
			setupFollowScaleOut(executorMapping, oldExecutorMapping);
		} else if (newParallelism < oldParallelism) {
			isScaling = true;
			setupFollowScaleIn(executorMapping, oldExecutorMapping);
		} else {
			isScaling = false;
			setupFollowRepartition(executorMapping, oldExecutorMapping);
		}
		fillingUnused(executorMapping.keySet().size());

		generateAlignedKeyGroupRanges();
		generateExecutorIdMapping();
	}

	public OperatorWorkloadsAssignment(
		Map<Integer, List<Integer>> executorMapping,
		int numOpenedSubtask) {

		this.numOpenedSubtask = numOpenedSubtask;
		this.oldRescalePA = null;

		checkState(checkPartitionAssignmentValidity(executorMapping),
			"executorMapping has null or empty partition");

		this.partitionAssignment = executorMapping;
		this.subtaskIndexMapping = initSubtaskIndexMap(numOpenedSubtask);

		this.executorIdMapping = new HashMap<>();
		this.alignedKeyGroupRanges = new ArrayList<>();
		this.modifiedSubtaskMap = new HashMap<>();
		this.removedSubtaskMap = new HashMap<>();

		isScaling = false;

		generateAlignedKeyGroupRanges();
		generateExecutorIdMapping();
	}

	private void setupFollowScaleOut(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping) {

		List<Integer> createdIdList = executorMapping.keySet().stream()
			.filter(id -> !oldExecutorMapping.containsKey(id))
			.collect(Collectors.toList());
//		checkState(createdIdList.size() == 1, "more than one created");

//		int createdExecutorId = createdIdList.get(0);

		List<Integer> modifiedIdList = oldExecutorMapping.keySet().stream()
			.filter(id -> oldExecutorMapping.get(id).size() != executorMapping.get(id).size())
			.collect(Collectors.toList());
//		checkState(modifiedIdList.size() == 1, "more than one modified in scale out");

//		int modifiedExecutorId = modifiedIdList.get(0);

		Map<Integer, Integer> unUsedSubtaskList = findNextUnusedSubtask(createdIdList);

		for (Map.Entry<Integer, List<Integer>> entry : executorMapping.entrySet()) {
			int executorId = entry.getKey();
			List<Integer> partition = entry.getValue();

			int subtaskIndex = (createdIdList.contains(executorId)) ?
				unUsedSubtaskList.get(executorId):
				oldRescalePA.getSubTaskId(executorId);

			putExecutorToSubtask(subtaskIndex, executorId, partition);

//			if (executorId == createdExecutorId || executorId == modifiedExecutorId) {
			if (createdIdList.contains(executorId) || modifiedIdList.contains(executorId)) {
				modifiedSubtaskMap.put(subtaskIndex, true);
			}
		}
	}

	private void setupFollowScaleIn(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping) {

		List<Integer> removedIdList = oldExecutorMapping.keySet().stream()
			.filter(id -> !executorMapping.containsKey(id))
			.collect(Collectors.toList());
//		checkState(removedIdList.size() == 1, "more than one removed");

//		int removedId = removedIdList.get(0);
//		modifiedSubtaskMap.put(oldRescalePA.getSubTaskId(removedId), true);
//		removedSubtaskMap.put(oldRescalePA.getSubTaskId(removedId), true);
		for (int removedId : removedIdList) {
			modifiedSubtaskMap.put(oldRescalePA.getSubTaskId(removedId), true);
			removedSubtaskMap.put(oldRescalePA.getSubTaskId(removedId), true);
		}

		List<Integer> modifiedIdList = executorMapping.keySet().stream()
			.filter(id -> executorMapping.get(id).size() != oldExecutorMapping.get(id).size())
			.collect(Collectors.toList());
//		checkState(modifiedIdList.size() == 1, "more than one modified in scale in");

//		int modifiedExecutorId = modifiedIdList.get(0);

		for (Map.Entry<Integer, List<Integer>> entry : executorMapping.entrySet()) {
			int executorId = entry.getKey();
			List<Integer> partition = entry.getValue();

			int subtaskIndex = oldRescalePA.getSubTaskId(executorId);
			putExecutorToSubtask(subtaskIndex, executorId, partition);

			if (modifiedIdList.contains(executorId) || removedIdList.contains(executorId)) {
				modifiedSubtaskMap.put(subtaskIndex, true);
			}
		}
	}

	private void setupFollowRepartition(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping) {

		List<Integer> modifiedIdList = executorMapping.keySet().stream()
			.filter(id -> executorMapping.get(id).size() != oldExecutorMapping.get(id).size())
			.collect(Collectors.toList());
//		checkState(modifiedIdList.size() == 2, "not exactly two are modified in repartition");

		for (Map.Entry<Integer, List<Integer>> entry : executorMapping.entrySet()) {
			int executorId = entry.getKey();
			List<Integer> partition = entry.getValue();

			int subtaskIndex = oldRescalePA.getSubTaskId(executorId);
			putExecutorToSubtask(subtaskIndex, executorId, partition);

			if (modifiedIdList.contains(executorId)) {
				modifiedSubtaskMap.put(subtaskIndex, true);
			}
		}
	}

	private Map<Integer, Integer> findNextUnusedSubtask(List<Integer> createdIdList) {
		checkState(createdIdList.size() > 0, "null created task list");

		int n = createdIdList.size();
		Map<Integer, Integer> subtaskIndex = new HashMap<>(n);
		for (int i = 0; i < numOpenedSubtask; i++) {
			if (oldRescalePA.getIdInModel(i) == UNUSED_SUBTASK) {
				subtaskIndex.put(createdIdList.get(createdIdList.size()-n), i);
				n--;
				if (n == 0) {
					break;
				}
			}
		}
		checkState(subtaskIndex.size() > 0, "cannot find valid subtask for created executor");

		return subtaskIndex;
	}

	private void putExecutorToSubtask(int subtaskIndex, int executorId, List<Integer> partition) {
		Integer absent = subtaskIndexMapping.putIfAbsent(subtaskIndex, executorId);
		checkState(absent == null, "should be one-to-one mapping " + absent);

		List<Integer> absent1 = partitionAssignment.putIfAbsent(subtaskIndex, partition);
		checkState(absent1 == null, "should be one-to-one mapping " + absent1);
	}

//	private void fillingUnused(int newParallelism) {
//		int numOccupiedSubtask = 0;
//		for (int subtaskIndex = 0; subtaskIndex < numOpenedSubtask; subtaskIndex++) {
//			Integer absent = subtaskIndexMapping.putIfAbsent(subtaskIndex, UNUSED_SUBTASK);
//			partitionAssignment.putIfAbsent(subtaskIndex, new ArrayList<>());
//
//			if (absent != null) {
//				numOccupiedSubtask++;
//			}
//		}
//
//		checkState(numOccupiedSubtask == newParallelism);
//	}

	private void fillingUnused(int newParallelism) {
		int numOccupiedSubtask = 0;
		Map<Integer, Integer> newSubtaskIndexMapping = new HashMap<>();
		Map<Integer, List<Integer>> newPartitionAssignment = new HashMap<>();
		Map<Integer, Boolean> newModifiedSubtaskMap = new HashMap<>();
		for (int subtaskIndex = 0; subtaskIndex < numOpenedSubtask; subtaskIndex++) {
			Integer absent = subtaskIndexMapping.putIfAbsent(subtaskIndex, UNUSED_SUBTASK);
			partitionAssignment.putIfAbsent(subtaskIndex, new ArrayList<>());
			if (absent != null) {
				newSubtaskIndexMapping.put(numOccupiedSubtask, subtaskIndexMapping.get(subtaskIndex));
				newPartitionAssignment.put(numOccupiedSubtask, partitionAssignment.get(subtaskIndex));
				if (modifiedSubtaskMap.containsKey(subtaskIndex)) {
					newModifiedSubtaskMap.put(numOccupiedSubtask, true);
				}
				numOccupiedSubtask++;
			}
		}

		checkState(numOccupiedSubtask == newParallelism);

		subtaskIndexMapping = newSubtaskIndexMapping;
		partitionAssignment = newPartitionAssignment;
		modifiedSubtaskMap = newModifiedSubtaskMap;
		numOpenedSubtask = newPartitionAssignment.size();

		LOG.info("++++++ subtaskIndexMapping: " + subtaskIndexMapping);
		LOG.info("++++++ partitionAssignment: " + partitionAssignment);
		LOG.info("++++++ modifiedSubtaskMap: " + modifiedSubtaskMap);
		LOG.info("++++++ removedSubtaskMap: " + removedSubtaskMap);
	}

	private void generateAlignedKeyGroupRanges() {
		int keyGroupStart = 0;
		for (int subTaskIndex = 0; subTaskIndex < partitionAssignment.keySet().size(); subTaskIndex++) {
			int rangeSize = partitionAssignment.get(subTaskIndex).size();

			KeyGroupRange keyGroupRange = rangeSize == 0 ?
				KeyGroupRange.EMPTY_KEY_GROUP_RANGE :
				new KeyGroupRange(
					keyGroupStart,
					keyGroupStart + rangeSize - 1,
					partitionAssignment.get(subTaskIndex));

			alignedKeyGroupRanges.add(keyGroupRange);
			keyGroupStart += rangeSize;
		}
	}

	private void generateExecutorIdMapping() {
		for (Map.Entry<Integer, Integer> entry : subtaskIndexMapping.entrySet()) {
			if (entry.getValue() != UNUSED_SUBTASK) {
				executorIdMapping.put(entry.getValue(), entry.getKey());
			}
		}
	}

	public int getNumOpenedSubtask() {
		return numOpenedSubtask;
	}

	public int getIdInModel(int subtaskIndex) {
		return subtaskIndexMapping.getOrDefault(subtaskIndex, UNUSED_SUBTASK);
	}

	public int getSubTaskId(int idInModel) {
		return executorIdMapping.get(idInModel);
	}

	public Map<Integer, List<Integer>> getPartitionAssignment() {
		return partitionAssignment;
	}

	public List<KeyGroupRange> getAlignedKeyGroupRanges() {
		return alignedKeyGroupRanges;
	}

	public KeyGroupRange getAlignedKeyGroupRange(int subTaskIndex) {
		return alignedKeyGroupRanges.get(subTaskIndex);
	}

	public boolean isTaskModified(int subtaskIndex) {
		return modifiedSubtaskMap.getOrDefault(subtaskIndex, false);
	}

	public List<Integer> getRemovedSubtask() {
		return new ArrayList<>(removedSubtaskMap.keySet());
	}


	private static boolean checkPartitionAssignmentValidity(
		Map<Integer, List<Integer>> partitionAssignment) {

		for (List<Integer> partitions : partitionAssignment.values()) {
			if (partitions == null || partitions.size() == 0) {
				return false;
			}
		}
		return true;
	}

	// map of string -> map of integer
	private static Map<Integer, List<Integer>> generateIntegerMap(
		Map<String, List<String>> partitionAssignment) {

		Map<Integer, List<Integer>> mapping = new HashMap<>();
		for (String subTaskIndexStr : partitionAssignment.keySet()) {
			int subTaskIndex = Integer.parseInt(subTaskIndexStr);
			List<Integer> partitions = new ArrayList<>();

			for (String partitionStr : partitionAssignment.get(subTaskIndexStr)) {
				partitions.add(Integer.parseInt(partitionStr));
			}
			mapping.put(subTaskIndex, partitions);
		}

		return mapping;
	}

	private static Map<Integer, Integer> initSubtaskIndexMap(
		int numExecutors) {

		Map<Integer, Integer> mapping = new HashMap<>();
		for (int i = 0; i < numExecutors; i++) {
			mapping.put(i, i);
		}
		return mapping;
	}

	@Override
	public String toString() {
		return String.format("\n%s: %s\n%s: %s\n%s: %s\n%s: %s",
			"partitionAssignment", partitionAssignment,
			"subtaskIndexMapping", subtaskIndexMapping,
			"alignedKeyGroupRanges", alignedKeyGroupRanges,
			"modifiedSubtaskMap", modifiedSubtaskMap);
	}

	public boolean isScaling() {
		return isScaling;
	}
}

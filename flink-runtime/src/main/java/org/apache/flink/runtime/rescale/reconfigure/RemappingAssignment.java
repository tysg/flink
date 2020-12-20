package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemappingAssignment implements AbstractCoordinator.Diff {

	private final Map<Integer, List<Integer>> keymapping = new HashMap<>();
	private final Map<Integer, List<Integer>> oldKeymapping = new HashMap<>();
	private final List<KeyGroupRange> alignedKeyGroupRanges;

	public RemappingAssignment(List<List<Integer>> newMapping, List<List<Integer>> oldMapping) {
		for (int i = 0; i < newMapping.size(); i++) {
			keymapping.put(i, newMapping.get(i));
		}
		for (int i = 0; i < oldMapping.size(); i++) {
			oldKeymapping.put(i, oldMapping.get(i));
		}
		alignedKeyGroupRanges = generateAlignedKeyGroupRanges(newMapping);
	}

	public RemappingAssignment(List<List<Integer>> mapping) {
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

	public boolean isTaskModified(int taskIndex){
		if(taskIndex >= oldKeymapping.size()){
			return true;
		}
		return AbstractCoordinator.compareIntList(oldKeymapping.get(taskIndex), keymapping.get(taskIndex));
	}

	public Map<Integer, List<Integer>> getPartitionAssignment() {
		return keymapping;
	}

	public KeyGroupRange getAlignedKeyGroupRange(int subTaskIndex) {
		return alignedKeyGroupRanges.get(subTaskIndex);
	}
}

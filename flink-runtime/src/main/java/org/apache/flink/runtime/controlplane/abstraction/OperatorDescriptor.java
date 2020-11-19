package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * this class make sure all field is not modifiable for external class
 */
public class OperatorDescriptor {

	private final Integer operatorID;
	private final String name;

	private final Set<OperatorDescriptor> parents = new HashSet<>();
	private final Set<OperatorDescriptor> children = new HashSet<>();

	private final OperatorPayload payload;

	public OperatorDescriptor(Integer operatorID, String name, int parallelism) {
		this.operatorID = operatorID;
		this.name = name;
		this.payload = new OperatorPayload(parallelism);
	}

	public Integer getOperatorID() {
		return operatorID;
	}

	public String getName() {
		return name;
	}

	public Set<OperatorDescriptor> getParents() {
		return Collections.unmodifiableSet(parents);
	}

	public Set<OperatorDescriptor> getChildren() {
		return Collections.unmodifiableSet(children);
	}

	public int getParallelism() {
		return payload.parallelism;
	}

	public Function getUdf() {
		return Preconditions.checkNotNull(payload.udf);
	}

	public Map<Integer, List<List<Integer>>> getKeyStateAllocation() {
		return Collections.unmodifiableMap(payload.keyStateAllocation);
	}

	public Map<Integer, List<List<Integer>>> getKeyMapping() {
		return Collections.unmodifiableMap(payload.keyMapping);
	}

	public void setParallelism(int parallelism) {
		payload.parallelism = parallelism;
	}

	public void setUdf(Function udf) {
		payload.udf = udf;
	}

	public void setKeyStateAllocation(Map<Integer, List<List<Integer>>> keyStateAllocation) {
		Map<Integer, List<List<Integer>>> unmodifiable = convertToUnmodifiable(keyStateAllocation);
		payload.keyStateAllocation.putAll(unmodifiable);
		for (OperatorDescriptor parent : parents) {
			parent.payload.keyMapping.put(operatorID, unmodifiable.get(parent.operatorID));
		}
	}

	private Map<Integer, List<List<Integer>>> convertToUnmodifiable(Map<Integer, List<List<Integer>>> keyStateAllocation) {
		Map<Integer, List<List<Integer>>> unmodifiable = new HashMap<>();
		for (Integer inOpID : keyStateAllocation.keySet()) {
			List<List<Integer>> unmodifiableKeys = keyStateAllocation.get(inOpID)
				.stream()
				.map(Collections::unmodifiableList)
				.collect(Collectors.toList());
			unmodifiable.put(inOpID, Collections.unmodifiableList(unmodifiableKeys));
		}
		return unmodifiable;
	}

	/**
	 * we assume that the key operator only have one operator
	 *
	 * @param keyStateAllocation
	 */
	@VisibleForTesting
	public void setKeyStateAllocation(List<List<Integer>> keyStateAllocation) {
		if (parents.size() != 1) {
			System.out.println("not support now");
			return;
		}
		OperatorDescriptor parent = (OperatorDescriptor) parents.toArray()[0];
		List<List<Integer>> unmodifiableKeys = keyStateAllocation.stream()
			.map(Collections::unmodifiableList)
			.collect(Collectors.toList());
		unmodifiableKeys = Collections.unmodifiableList(unmodifiableKeys);

		payload.keyStateAllocation.put(parent.getOperatorID(), unmodifiableKeys);
		// sync with parent's key mapping
		parent.payload.keyMapping.put(operatorID, unmodifiableKeys);
	}

	/**
	 * @param childEdges       the list of pair of parent id and child id to represent the relationship between operator
	 * @param allOperatorsById
	 */
	public void addChildren(List<Tuple2<Integer, Integer>> childEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		// f0 is parent operator id, f1 is child operator id
		for (Tuple2<Integer, Integer> edge : childEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.f0) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.f1);
			// I think I am your father
			children.add(descriptor);
			descriptor.parents.add(this);
		}
	}

	/**
	 * @param parentEdges      the list of pair of parent id and child id to represent the relationship between operator
	 * @param allOperatorsById
	 */
	public void addParent(List<Tuple2<Integer, Integer>> parentEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		// f0 is parent operator id, f1 is child operator id
		for (Tuple2<Integer, Integer> edge : parentEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.f1) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.f0);
			// I think I am your father
			parents.add(descriptor);
			descriptor.children.add(this);
		}
	}

	@Override
	public String toString() {
		return "OperatorDescriptor{name='" + name + "\'', parallelism=" + payload.parallelism +
			", parents:" + parents.size() + ", children:" + children.size() + '}';
	}

	private static class OperatorPayload {
		int parallelism;
		Function udf;

		final Map<Integer, List<List<Integer>>> keyStateAllocation = new HashMap<>();
		final Map<Integer, List<List<Integer>>> keyMapping = new HashMap<>();

		OperatorPayload(int parallelism) {
			this.parallelism = parallelism;
		}
	}

}

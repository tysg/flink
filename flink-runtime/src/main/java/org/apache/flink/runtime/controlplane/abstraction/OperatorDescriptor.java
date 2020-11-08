package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		return parents;
	}

	public Set<OperatorDescriptor> getChildren() {
		return children;
	}

	public int getParallelism() {
		return payload.parallelism;
	}

	public Function getUdf() {
		return Preconditions.checkNotNull(payload.udf);
	}

	public Map<Integer, List<List<Integer>>> getKeyStateAllocation() {
		return Preconditions.checkNotNull(payload.keyStateAllocation);
	}

	public Map<Integer, List<List<Integer>>> getKeyMapping() {
		return Preconditions.checkNotNull(payload.keyMapping);
	}

	public void setParallelism(int parallelism) {
		payload.parallelism = parallelism;
	}

	public void setUdf(Function udf) {
		payload.udf = udf;
	}

	public void setKeyStateAllocation(Map<Integer, List<List<Integer>>> keyStateAllocation) {
		payload.keyStateAllocation = keyStateAllocation;
	}

	public void setKeyMapping(Map<Integer, List<List<Integer>>> keyMapping) {
		payload.keyMapping = keyMapping;
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

		Map<Integer, List<List<Integer>>> keyStateAllocation;
		Map<Integer, List<List<Integer>>> keyMapping;

		OperatorPayload(int parallelism) {
			this.parallelism = parallelism;
		}
	}

}

package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;

@Internal
public class OperatorDescriptorBuilder {

	private OperatorDescriptor operatorDescriptor;

	private final static OperatorDescriptorBuilder INSTANCE = new OperatorDescriptorBuilder();

	public static OperatorDescriptorBuilder attachOperator(OperatorDescriptor operatorDescriptor) {
		INSTANCE.operatorDescriptor = operatorDescriptor;
		return INSTANCE;
	}

	public void addChildren(List<Tuple2<Integer, Integer>> childEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		operatorDescriptor.addChildren(childEdges, allOperatorsById);
	}

	public void addParent(List<Tuple2<Integer, Integer>> parentEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		operatorDescriptor.addParent(parentEdges, allOperatorsById);
	}

	public void setKeyStateAllocation(List<List<Integer>> keyStateAllocation) {
		operatorDescriptor.setKeyStateAllocation(keyStateAllocation);
	}

	public void setKeyMapping(Map<Integer, List<List<Integer>>> keyMapping) {
		operatorDescriptor.setKeyMapping(keyMapping);
	}
}

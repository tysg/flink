package org.apache.flink.streaming.controlplane.streammanager.insts;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class OperatorDescriptor {

	private final OperatorID operatorID;
	private final StreamConfig streamConfig;
	private final String name;

	private final Set<OperatorDescriptor> parents = new HashSet<>();
	private final Set<OperatorDescriptor> children = new HashSet<>();

	private int parallelism;

	OperatorDescriptor(OperatorID operatorID, StreamConfig config, int parallelism) {
		this.operatorID = operatorID;
		this.streamConfig = config;
		this.parallelism = parallelism;
		this.name = config.getOperatorName();
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	public StreamConfig getStreamConfig() {
		return streamConfig;
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
		return parallelism;
	}

	void addChildren(List<StreamEdge> childEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		for (StreamEdge edge : childEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.getSourceId()) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.getTargetId());
			// I think I am your father
			children.add(descriptor);
			descriptor.parents.add(this);
		}
	}

	public void addParent(List<StreamEdge> parentEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		for (StreamEdge edge : parentEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.getTargetId()) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.getSourceId());
			// I think I am your father
			parents.add(descriptor);
			descriptor.children.add(this);
		}
	}

	@Override
	public String toString() {
		return "OperatorDescriptor{name='" + name + "\'', parallelism=" + parallelism +
			", parents:" + parents.size() + ", children:" + children.size() + '}';
	}

}

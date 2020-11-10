/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.streammanager.insts;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionGraphConfig;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.AssignedKeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.stream.Collectors;

public final class StreamJobExecutionPlanImpl implements StreamJobExecutionPlan {

	private final Map<Integer, OperatorDescriptor> allOperatorsById = new LinkedHashMap<>();
	private final OperatorDescriptor[] heads;

	private final Map<Integer, List<ExecutionGraphConfig.OperatorTask>> operatorTaskListMap = new HashMap<>();
	private final Collection<Host> hosts;

	@Internal
	public StreamJobExecutionPlanImpl(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader userLoader) {
		heads = initialOperatorGraphState(jobGraph, userLoader);

		Map<OperatorID, Integer> operatorIdToVertexId = new HashMap<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(userLoader);
			for (StreamConfig config : configMap.values()) {
				operatorIdToVertexId.put(config.getOperatorID(), config.getVertexID());
			}
		}
		hosts = initDeploymentGraphState(executionGraph, operatorIdToVertexId);
	}

	// OperatorGraphState related
	/* contains topology of this stream job */
	private OperatorDescriptor[] initialOperatorGraphState(JobGraph jobGraph, ClassLoader userCodeLoader) {
		// add all nodes
		Map<Integer, StreamConfig> streamConfigMap = new HashMap<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(userCodeLoader);
			streamConfigMap.putAll(configMap);
			for (StreamConfig config : configMap.values()) {
				OperatorDescriptor operatorDescriptor = new OperatorDescriptor(
					config.getVertexID(), config.getOperatorName(), vertex.getParallelism());
				allOperatorsById.put(config.getVertexID(), operatorDescriptor);

			}
		}
		// build topology
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			StreamConfig config = streamConfigMap.get(descriptor.getOperatorID());
			List<Tuple2<Integer, Integer>> outEdges = config.getOutEdges(userCodeLoader)
				.stream()
				.map(e -> Tuple2.of(e.getSourceId(), e.getTargetId()))
				.collect(Collectors.toList());
			descriptor.addChildren(outEdges, allOperatorsById);

			List<Tuple2<Integer, Integer>> inEdges = config.getInPhysicalEdges(userCodeLoader)
				.stream()
				.map(e -> Tuple2.of(e.getSourceId(), e.getTargetId()))
				.collect(Collectors.toList());
			descriptor.addParent(inEdges, allOperatorsById);
		}
		// find head
		List<OperatorDescriptor> heads = new ArrayList<>();
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			if (descriptor.getParents().isEmpty()) {
				heads.add(descriptor);
			}
		}
		checkRelationship();
		// finished other field
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			StreamConfig streamConfig = streamConfigMap.get(descriptor.getOperatorID());
			Function function = getUserFunction(streamConfigMap.get(descriptor.getOperatorID()), userCodeLoader);
			Map<Integer, List<List<Integer>>> keyStateAllocation = getKeyStateAllocation(streamConfig, userCodeLoader);
			descriptor.setUdf(function);
			descriptor.setKeyStateAllocation(keyStateAllocation);
		}
		// finished key mapping
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			Map<Integer, List<List<Integer>>> keyMapping = getKeyMappingInternal(descriptor.getOperatorID());
			descriptor.setKeyMapping(keyMapping);
		}
		return heads.toArray(new OperatorDescriptor[0]);
	}

	@Override
	public int getParallelism(Integer operatorID) {
		return this.allOperatorsById.get(operatorID).getParallelism();
	}

	@Override
	public Function getUserFunction(Integer operatorID) {
		return allOperatorsById.get(operatorID).getUdf();
	}

	@Override
	public Map<Integer, List<List<Integer>>> getKeyStateAllocation(Integer operatorID) {
		return allOperatorsById.get(operatorID).getKeyStateAllocation();
	}

	@Override
	public Map<Integer, List<List<Integer>>> getKeyMapping(Integer operatorID) {
		return allOperatorsById.get(operatorID).getKeyMapping();
	}

	@Override
	public Iterator<OperatorDescriptor> getAllOperatorDescriptor() {
		return allOperatorsById.values().iterator();
	}

	public OperatorDescriptor[] getHeads() {
		return heads;
	}

	private Function getUserFunction(StreamConfig config, ClassLoader userCodeLoader) {
		StreamOperatorFactory<?> factory = config.getStreamOperatorFactory(userCodeLoader);
		if (factory instanceof SimpleUdfStreamOperatorFactory) {
			return ((SimpleUdfStreamOperatorFactory<?>) factory).getUserFunction();
		}
		return null;
	}

	private Map<Integer, List<List<Integer>>> getKeyStateAllocation(StreamConfig config, ClassLoader userCodeLoader) {
		Integer targetOperatorId = config.getVertexID();
		Map<Integer, List<List<Integer>>> res = new HashMap<>();
		List<StreamEdge> inPhysicalEdges = config.getInPhysicalEdges(userCodeLoader);
		for (StreamEdge edge : inPhysicalEdges) {
			res.put(edge.getSourceId(), getKeyMessage(edge, allOperatorsById.get(targetOperatorId).getParallelism()));
		}
		return res;
	}

	private Map<Integer, List<List<Integer>>> getKeyMappingInternal(Integer operatorID) {
		Map<Integer, List<List<Integer>>> res = new HashMap<>();
		OperatorDescriptor targetOp = allOperatorsById.get(operatorID);
		for (OperatorDescriptor child : targetOp.getChildren()) {
			res.put(child.getOperatorID(), getKeyStateAllocation(child.getOperatorID()).get(targetOp.getOperatorID()));
		}
		return res;
	}

	private List<List<Integer>> getKeyMessage(StreamEdge streamEdge, int parallelism) {
		StreamPartitioner<?> partitioner = streamEdge.getPartitioner();
		if (partitioner instanceof AssignedKeyGroupStreamPartitioner) {
			return ((AssignedKeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo(parallelism);
		} else if (partitioner instanceof KeyGroupStreamPartitioner) {
			return ((KeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo(parallelism);
		}
		// it may be not a key stream operator
		return Collections.emptyList();
	}

	private void checkRelationship() {
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			for (OperatorDescriptor child : descriptor.getChildren()) {
				Preconditions.checkArgument(child.getParents().contains(descriptor), child + "'s parents should contain " + descriptor);
			}
			for (OperatorDescriptor parent : descriptor.getParents()) {
				Preconditions.checkArgument(parent.getChildren().contains(descriptor), parent + "'s children should contain " + descriptor);
			}
		}
	}

	// DeployGraphState related
	private Collection<Host> initDeploymentGraphState(ExecutionGraph executionGraph, Map<OperatorID, Integer> operatorIdToVertexId) {
		Map<ResourceID, org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.Host> hosts = new HashMap<>();

		for (ExecutionJobVertex jobVertex : executionGraph.getAllVertices().values()) {
			// contains all tasks of the same parallel operator instances
			List<ExecutionGraphConfig.OperatorTask> operatorTaskList = new ArrayList<>(jobVertex.getParallelism());
			for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
				LogicalSlot slot;
				do {
					slot = vertex.getCurrentAssignedResource();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} while (slot == null);

				org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.Host host = hosts.get(slot.getTaskManagerLocation().getResourceID());
				if (host == null) {
					host = new org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.Host(slot.getTaskManagerLocation().address(), 0, 0);
					hosts.put(slot.getTaskManagerLocation().getResourceID(), host);
				}
				// todo how to get cpu, number of threads, memory?
				org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.OperatorTask operatorTask = new org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.OperatorTask(slot.getPhysicalSlotNumber(), host);
				operatorTaskList.add(operatorTask);
			}
			for (OperatorID operatorID : jobVertex.getOperatorIDs()) {
				operatorTaskListMap.put(operatorIdToVertexId.get(operatorID), operatorTaskList);
			}
		}
		return hosts.values();
	}

	@Override
	public org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.Host[] getHosts() {
		return hosts.toArray(new org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.Host[0]);
	}

	@Override
	public org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.OperatorTask getTask(Integer operatorID, int offset) {
		return operatorTaskListMap.get(operatorID).get(offset);
	}

}

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
import org.apache.flink.runtime.controlplane.abstraction.*;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
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
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.AssignedKeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public final class StreamJobExecutionPlanImpl implements StreamJobExecutionPlan {

	private final Map<Integer, OperatorDescriptor> allOperatorsById = new LinkedHashMap<>();
	private final OperatorDescriptor[] heads;

	private final Map<Integer, List<ExecutionGraphConfig.OperatorTask>> operatorTaskListMap = new HashMap<>();
	private final List<Host> hosts;

	@Internal
	public StreamJobExecutionPlanImpl(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader userLoader) {
		heads = initializeOperatorGraphState(jobGraph, userLoader);

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
	private OperatorDescriptor[] initializeOperatorGraphState(JobGraph jobGraph, ClassLoader userCodeLoader) {
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
			OperatorDescriptorVisitor.attachOperator(descriptor).addChildren(outEdges, allOperatorsById);

			List<Tuple2<Integer, Integer>> inEdges = config.getInPhysicalEdges(userCodeLoader)
				.stream()
				.map(e -> Tuple2.of(e.getSourceId(), e.getTargetId()))
				.collect(Collectors.toList());
			OperatorDescriptorVisitor.attachOperator(descriptor).addParent(inEdges, allOperatorsById);
		}
		// find head
		List<OperatorDescriptor> heads = new ArrayList<>();
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			if (descriptor.getParents().isEmpty()) {
				heads.add(descriptor);
			}
		}
		checkRelationship(allOperatorsById.values());
		// finished other field
		for (OperatorDescriptor descriptor : allOperatorsById.values()) {
			StreamConfig streamConfig = streamConfigMap.get(descriptor.getOperatorID());
			// add workload dimension info
			Map<Integer, List<Integer>> keyStateAllocation = getKeyStateAllocation(streamConfig, userCodeLoader, descriptor.getParallelism());
			OperatorDescriptorVisitor.attachOperator(descriptor).setKeyStateAllocation(keyStateAllocation);
			// add execution logic info
			try {
				attachAppLogicToOperatorDescriptor(descriptor, streamConfigMap.get(descriptor.getOperatorID()), userCodeLoader);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
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
	public Map<Integer, List<Integer>> getKeyStateAllocation(Integer operatorID) {
		return allOperatorsById.get(operatorID).getKeyStateAllocation();
	}

	@Override
	public Map<Integer, Map<Integer, List<Integer>>> getKeyMapping(Integer operatorID) {
		return allOperatorsById.get(operatorID).getKeyMapping();
	}

	@Override
	public Iterator<OperatorDescriptor> getAllOperatorDescriptor() {
		return allOperatorsById.values().iterator();
	}

	@Override
	public OperatorDescriptor getOperatorDescriptorByID(Integer operatorID) {
		return allOperatorsById.get(operatorID);
	}

	public OperatorDescriptor[] getHeads() {
		return heads;
	}

	// DeployGraphState related
	private List<Host> initDeploymentGraphState(ExecutionGraph executionGraph, Map<OperatorID, Integer> operatorIdToVertexId) {
		Map<ResourceID, org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan.Host> hosts = new HashMap<>();

		for (ExecutionJobVertex jobVertex : executionGraph.getAllVertices().values()) {
			// contains all tasks of the same parallel operator instances
			List<ExecutionGraphConfig.OperatorTask> operatorTaskList = new ArrayList<>(jobVertex.getParallelism());
			for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
				Execution execution;
				do {
					// TODO: special check for scale out, which must have null execution
					execution = vertex.getCurrentExecutionAttempt();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} while (execution == null || execution.getState() != ExecutionState.RUNNING);
				LogicalSlot slot = execution.getAssignedResource();
				Host host = hosts.get(slot.getTaskManagerLocation().getResourceID());
				if (host == null) {
					host = new Host(slot.getTaskManagerLocation().address(), 0, 0);
					hosts.put(slot.getTaskManagerLocation().getResourceID(), host);
				}
				// todo how to get cpu, number of threads, memory?
				OperatorTask operatorTask = new OperatorTask(slot.getPhysicalSlotNumber(), host);
				operatorTaskList.add(operatorTask);
			}
			for (OperatorID operatorID : jobVertex.getOperatorIDs()) {
				operatorTaskListMap.put(operatorIdToVertexId.get(operatorID), operatorTaskList);
			}
		}
		return new ArrayList<>(hosts.values());
	}

	@Override
	public Host[] getHosts() {
		return hosts.toArray(new Host[0]);
	}

	@Override
	public OperatorTask getTask(Integer operatorID, int offset) {
		return operatorTaskListMap.get(operatorID).get(offset);
	}

	private static void attachAppLogicToOperatorDescriptor(
		OperatorDescriptor descriptor,
		StreamConfig config,
		ClassLoader userCodeLoader) throws IllegalAccessException {

		StreamOperatorFactory<?> factory = config.getStreamOperatorFactory(userCodeLoader);
		if (factory instanceof SimpleUdfStreamOperatorFactory) {
			Function function = ((SimpleUdfStreamOperatorFactory<?>) factory).getUserFunction();
			descriptor.setUdf(function);
			StreamOperator<?> streamOperator = ((SimpleUdfStreamOperatorFactory<?>) factory).getOperator();
			Class<?> streamOperatorClass = streamOperator.getClass();
			List<Field> fieldList = new LinkedList<>();
			do {
				fieldList.addAll(
					Arrays.stream(streamOperatorClass.getDeclaredFields())
						.filter(field -> field.isAnnotationPresent(ControlAttribute.class))
						.collect(Collectors.toList())
				);
 				streamOperatorClass = streamOperatorClass.getSuperclass();
			} while (streamOperatorClass != null);
			OperatorDescriptorVisitor.attachOperator(descriptor).setAttributeField(streamOperator, fieldList);
		}
	}

	private static Map<Integer, List<Integer>> getKeyStateAllocation(StreamConfig config, ClassLoader userCodeLoader, int parallelism) {
		Map<Integer, Map<Integer, List<Integer>>> res = new HashMap<>();
		// very strange that some operator's out physical edge may not be completed
		List<StreamEdge> inPhysicalEdges = config.getInPhysicalEdges(userCodeLoader);
		for (StreamEdge edge : inPhysicalEdges) {
			res.put(edge.getSourceId(), getKeyMessage(edge, parallelism));
		}
		if (res.isEmpty()) {
//			throw new UnsupportedOperationException(
//				"Non-keyed stream is not supported, please use keyBy before operating on streams.");
			return new HashMap<>();
		} else {
			// todo, check all key set is same
			return res.values().iterator().next();
		}
	}

	private static Map<Integer, List<Integer>> getKeyMessage(StreamEdge streamEdge, int parallelism) {
		StreamPartitioner<?> partitioner = streamEdge.getPartitioner();
		Map<String, List<String>> keyStateAllocation = new HashMap<>();
		if (partitioner instanceof AssignedKeyGroupStreamPartitioner) {
			return ((AssignedKeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo(parallelism);
		} else if (partitioner instanceof KeyGroupStreamPartitioner) {
			return ((KeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo(parallelism);
		}
		// the consumer operator may be not a key stream operator
		// We do not need to consider non-keyed stream, because it is hard to operate and manage and also out of our scope.
//		throw new UnsupportedOperationException(
//			"Non-keyed stream is not supported, please use keyBy before operating on streams.");
		return new HashMap<>();
	}

	private static void checkRelationship(Collection<OperatorDescriptor> allOp) {
		for (OperatorDescriptor descriptor : allOp) {
			for (OperatorDescriptor child : descriptor.getChildren()) {
				Preconditions.checkArgument(child.getParents().contains(descriptor), child + "'s parents should contain " + descriptor);
			}
			for (OperatorDescriptor parent : descriptor.getParents()) {
				Preconditions.checkArgument(parent.getChildren().contains(descriptor), parent + "'s children should contain " + descriptor);
			}
		}
	}

}

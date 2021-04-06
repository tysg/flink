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
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor.ExecutionLogic.UDF;

public final class ExecutionPlanImpl implements ExecutionPlan {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutionPlanImpl.class);
	// operatorId -> operator
	private final Map<Integer, OperatorDescriptor> operatorsMap;
	private final OperatorDescriptor[] headOperators;
	// operatorId -> task
	private final Map<Integer, Map<Integer, TaskDescriptor>> operatorToTaskMap;
	// node with resources
	private final List<Node> resourceDistribution;

	// transformation operations -> affected tasks grouped by operators.
	private final Map<String, Map<Integer, List<Integer>>> transformations = new HashMap<>();

	@Internal
	public ExecutionPlanImpl(Map<Integer, OperatorDescriptor> operatorsMap,
							 OperatorDescriptor[] headOperators,
							 Map<Integer, Map<Integer, TaskDescriptor>> operatorToTaskMap,
							 List<Node> resourceDistribution) {
		this.operatorsMap = operatorsMap;
		this.headOperators = headOperators;
		this.operatorToTaskMap = operatorToTaskMap;
		this.resourceDistribution = resourceDistribution;
	}

	@Override
	public int getParallelism(Integer operatorID) {
		return this.operatorsMap.get(operatorID).getParallelism();
	}

	@Override
	public Function getUserFunction(Integer operatorID) {
		return operatorsMap.get(operatorID).getUdf();
	}

	@Override
	public Map<Integer, List<Integer>> getKeyStateAllocation(Integer operatorID) {
		return operatorsMap.get(operatorID).getKeyStateAllocation();
	}

	@Override
	public Map<Integer, Map<Integer, List<Integer>>> getKeyMapping(Integer operatorID) {
		return operatorsMap.get(operatorID).getKeyMapping();
	}

	@Override
	public Iterator<OperatorDescriptor> getAllOperator() {
		return operatorsMap.values().iterator();
	}

	@Override
	public OperatorDescriptor getOperatorByID(Integer operatorID) {
		return operatorsMap.get(operatorID);
	}

	@Override
	public ExecutionPlan redistribute(Integer operatorID, Map<Integer, List<Integer>> distribution) {
		Preconditions.checkNotNull(getKeyStateAllocation(operatorID), "previous key state allocation should not be null");
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		// update the key set
//		targetDescriptor.updateKeyStateAllocation(distribution);
		Map<Integer, List<Integer>> remappingTasks = transformations.getOrDefault("remapping", new HashMap<>());
		for (OperatorDescriptor parent : targetDescriptor.getParents()) {
			parent.updateKeyMapping(operatorID, distribution);
			remappingTasks.put(parent.getOperatorID(), parent.getTaskIds());
		}
		// update the parallelism if the distribution key size is different
		if (targetDescriptor.getParallelism() != distribution.size()) {
			// add the downstream tasks that need to know the members update in the upstream.
			Map<Integer, List<Integer>> downStreamTasks = transformations.getOrDefault("downstream", new HashMap<>());
			targetDescriptor.setParallelism(distribution.size());
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> downStreamTasks.put(c.getOperatorID(), c.getTaskIds()));
			transformations.put("downstream", downStreamTasks);
		}
		// find out affected tasks, add them to transformations
		Map<Integer, List<Integer>> updateStateTasks = transformations.getOrDefault("redistribute", new HashMap<>());
		updateStateTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());

		transformations.put("redistribute", updateStateTasks);
		transformations.put("remapping", remappingTasks);
		return this;
	}

	@Override
	public ExecutionPlan updateExecutionLogic(Integer operatorID, Object function) {
		Preconditions.checkNotNull(getUserFunction(operatorID), "previous key state allocation should not be null");
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		try {
			targetDescriptor.setControlAttribute(UDF, function);
		} catch (Exception e) {
			LOG.info("update function failed.", e);
		}
		// find out affected tasks, add them to transformations
		Map<Integer, List<Integer>> updateFunctionTasks = transformations.getOrDefault("updateExecutionLogic", new HashMap<>());
		updateFunctionTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
		transformations.put("updateExecutionLogic", updateFunctionTasks);
		return this;
	}

	@Override
	public ExecutionPlan redeploy(Integer operatorID, @Nullable Map<Integer, Node> deployment, Boolean isCreate) {
		// TODO: deployment is null, default deployment, needs to assign tasks to nodes
		// TODO: deployment is nonnull, assign tasks to target Node with resources
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		if (deployment == null) {
		} else {
		}
		// find out affected tasks, add them to transformations, by far, we only need add all tasks into the set.
		if (isCreate) {
			// add to the value
			Map<Integer, List<Integer>> reDeployingTasks = transformations.getOrDefault("creating", new HashMap<>());
			reDeployingTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			transformations.put("creating", reDeployingTasks);
		} else {
			Map<Integer, List<Integer>> reDeployingTasks = transformations.getOrDefault("canceling", new HashMap<>());
			reDeployingTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			transformations.put("canceling", reDeployingTasks);
		}
		return this;
	}

	@Override
	// users need to implement their own update executionplan + add transformations
	public ExecutionPlan update(java.util.function.Function<ExecutionPlan, ExecutionPlan> applier) {
		return applier.apply(this);
	}

	@Override
	public Map<String, Map<Integer, List<Integer>>> getTransformations() {
		return transformations;
	}

	@Override
	public void clearTransformations() {
		transformations.clear();
	}

	public OperatorDescriptor[] getHeadOperators() {
		return headOperators;
	}

	@Override
	public Node[] getResourceDistribution() {
		return resourceDistribution.toArray(new Node[0]);
	}

	@Override
	public TaskDescriptor getTask(Integer operatorID, int taskId) {
		return operatorToTaskMap.get(operatorID).get(taskId);
	}

}

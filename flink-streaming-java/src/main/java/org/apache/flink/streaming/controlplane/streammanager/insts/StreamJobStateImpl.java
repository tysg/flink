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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.controlplane.reconfigure.JobGraphUpdater;
import org.apache.flink.streaming.controlplane.rescale.StreamJobGraphRescaler;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;
import org.apache.flink.streaming.runtime.partitioner.AssignedKeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class StreamJobStateImpl implements StreamJobState {

	private final static int COMMITTED = 1;
	private final static int STAGED = 0;

	private transient final JobGraph jobGraph;
	private transient final ExecutionGraph executionGraph;

	private final OperatorGraph operatorGraph;
	private final DeploymentGraph deploymentGraph;

	private final JobGraphUpdater jobGraphUpdater;

	private final ClassLoader userCodeLoader;

	private final AtomicInteger stateOfUpdate = new AtomicInteger(COMMITTED);

	private ControlPolicy currentWaitingController;


	private StreamJobStateImpl(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader userLoader) {
		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;
		this.userCodeLoader = userLoader;

		this.jobGraphUpdater = new JobGraphUpdater(
			new StreamJobGraphRescaler(jobGraph, userCodeLoader), jobGraph, userLoader);
		this.operatorGraph = new OperatorGraph();
		this.deploymentGraph = new DeploymentGraph();
	}

	public static StreamJobStateImpl createFromGraph(JobGraph jobGraph, ExecutionGraph executionGraph) {
		ClassLoader userLoader = executionGraph.getUserClassLoader();
		return new StreamJobStateImpl(jobGraph, executionGraph, userLoader);
	}

	@Override
	public OperatorGraph getOperatorGraph() {
		return operatorGraph;
	}

	@Override
	public void setStateUpdatingFlag(ControlPolicy waitingController) throws Exception {
		// some strategy needed here to ensure there is only one update at one time
		if (!stateOfUpdate.compareAndSet(COMMITTED, STAGED)) {
			throw new Exception("There is another state update not finished, the waiting controller is:" + currentWaitingController);
		}
		// the caller may want to wait the completion of this update.
		currentWaitingController = waitingController;
	}


	@Override
	public void notifyUpdateFinished(JobVertexID jobVertexID) throws Exception {
		if (stateOfUpdate.compareAndSet(STAGED, COMMITTED)) {
			if (currentWaitingController != null) {
				currentWaitingController.onChangeCompleted(jobVertexID);
			}
			return;
		}
		throw new Exception("There is not any state updating");
	}

	public <OUT> JobVertexID updateOperator(OperatorID operatorID,
											StreamOperatorFactory<OUT> operatorFactory,
											ControlPolicy waitingController) throws Exception {
		setStateUpdatingFlag(waitingController);
		return jobGraphUpdater.updateOperator(operatorID, operatorFactory);
	}

	public Function getUserFunction(OperatorID operatorID) throws Exception {
		return operatorGraph.getUserFunction(operatorID);
	}

	@Override
	public Map<String, List<List<Integer>>> getKeyStateAllocation(OperatorID operatorID) throws Exception {
		return operatorGraph.getKeyStateAllocation(operatorID);
	}

	@Override
	public Map<String, List<List<Integer>>> getKeyMapping(OperatorID operatorID) throws Exception {
		return operatorGraph.getKeyMapping(operatorID);
	}

	@Override
	public int getParallelism(OperatorID operatorID) {
		return operatorGraph.getParallelism(operatorID);
	}

	@Override
	public Iterator<OperatorDescriptor> getAllOperatorDescriptor() {
		return operatorGraph.getAllOperatorDescriptor();
	}

	@Override
	public Host[] getHosts() {
		return deploymentGraph.getHosts();
	}

	@Override
	public OperatorTask getTask(OperatorID operatorID, int offset) {
		return deploymentGraph.getTask(operatorID, offset);
	}

	@VisibleForTesting
	private StreamConfig findStreamConfig(OperatorID operatorID) throws Exception {
		for (JobVertex vertex : this.jobGraph.getVertices()) {
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(this.userCodeLoader);
			for (StreamConfig config : configMap.values()) {
				if (operatorID.equals(config.getOperatorID())) {
					return config;
				}
			}
		}
		throw new Exception("do not found target stream config with this operator id");
	}

	public void rescale() {

	}

	public class OperatorGraph implements OperatorGraphState {
		/* contains topology of this stream job */
		private final Map<OperatorID, OperatorDescriptor> allOperators = new LinkedHashMap<>();
		private final OperatorDescriptor[] heads;

		private OperatorGraph() {
			Map<Integer, OperatorDescriptor> allOperatorsById = new LinkedHashMap<>();
			// add all nodes
			for (JobVertex vertex : jobGraph.getVertices()) {
				StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
				Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(userCodeLoader);
				for (StreamConfig config : configMap.values()) {
					OperatorDescriptor operatorDescriptor = new OperatorDescriptor(
						config.getOperatorID(), config, vertex.getParallelism());
					allOperators.put(config.getOperatorID(), operatorDescriptor);
					allOperatorsById.put(config.getVertexID(), operatorDescriptor);
				}
			}
			// build topology
			for (OperatorDescriptor descriptor : allOperatorsById.values()) {
				StreamConfig config = descriptor.getStreamConfig();
				List<StreamEdge> outEdges = config.getOutEdges(userCodeLoader);
				allOperators.get(config.getOperatorID()).addChildren(outEdges, allOperatorsById);

				List<StreamEdge> inEdges = config.getInPhysicalEdges(userCodeLoader);
				allOperators.get(config.getOperatorID()).addParent(inEdges, allOperatorsById);
			}
			// find head
			List<OperatorDescriptor> heads = new ArrayList<>();
			for (OperatorDescriptor descriptor : allOperators.values()) {
				if (descriptor.getParents().isEmpty()) {
					heads.add(descriptor);
				}
			}
			checkRelationship();
			this.heads = heads.toArray(new OperatorDescriptor[0]);
		}

		public OperatorDescriptor[] getHeads() {
			return heads;
		}

		public Iterator<OperatorDescriptor> getAllOperatorDescriptor() {
			return allOperators.values().iterator();
		}

		@Override
		public Function getUserFunction(OperatorID operatorID) throws Exception {
			StreamConfig config = getStreamConfig(operatorID);
			return ((SimpleUdfStreamOperatorFactory<?>) config.getStreamOperatorFactory(userCodeLoader)).getUserFunction();
		}

		@Override
		public Map<String, List<List<Integer>>> getKeyStateAllocation(OperatorID operatorID) throws Exception {
			StreamConfig config = getStreamConfig(operatorID);

			Map<String, List<List<Integer>>> res = new HashMap<>();
			List<StreamEdge> inPhysicalEdges = config.getInPhysicalEdges(userCodeLoader);
			for (StreamEdge edge : inPhysicalEdges) {
				getKeyMessage(edge);
				res.put(edge.getEdgeId(), getKeyMessage(edge));
			}
			return res;
		}

		@Override
		public Map<String, List<List<Integer>>> getKeyMapping(OperatorID operatorID) throws Exception {
			StreamConfig config = getStreamConfig(operatorID);

			Map<String, List<List<Integer>>> res = new HashMap<>();
			List<StreamEdge> outEdges = config.getOutEdges(userCodeLoader);
			for (StreamEdge edge : outEdges) {
				getKeyMessage(edge);
				res.put(edge.getEdgeId(), getKeyMessage(edge));
			}
			return res;
		}

		private List<List<Integer>> getKeyMessage(StreamEdge streamEdge) {
			StreamPartitioner<?> partitioner = streamEdge.getPartitioner();
			if (partitioner instanceof AssignedKeyGroupStreamPartitioner) {
				return ((AssignedKeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo();
			} else if (partitioner instanceof KeyGroupStreamPartitioner) {
				return ((KeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo();
			}
			// it may be not a key stream operator
			return Collections.emptyList();
		}

		@Override
		public int getParallelism(OperatorID operatorID) {
			return this.allOperators.get(operatorID).getParallelism();
		}

		private StreamConfig getStreamConfig(OperatorID operatorID) throws Exception {
			OperatorDescriptor descriptor = Preconditions.checkNotNull(allOperators.get(operatorID), "can not found target operator");
			return descriptor.getStreamConfig();
		}

		private void checkRelationship() {
			for (OperatorDescriptor descriptor : allOperators.values()) {
				for (OperatorDescriptor child : descriptor.getChildren()) {
					Preconditions.checkArgument(child.getParents().contains(descriptor), child + "'s parents should contain " + descriptor);
				}
				for (OperatorDescriptor parent : descriptor.getParents()) {
					Preconditions.checkArgument(parent.getChildren().contains(descriptor), parent + "'s children should contain " + descriptor);
				}
			}
		}

	}

	public class DeploymentGraph implements DeployGraphState {

		private Map<OperatorID, List<DeployGraphState.OperatorTask>> operatorTaskListMap;
		private Map<ResourceID, Host> hosts;

		private DeploymentGraph() {
			operatorTaskListMap = new HashMap<>();
			hosts = new HashMap<>();

			for (ExecutionJobVertex jobVertex : executionGraph.getAllVertices().values()) {
				// contains all tasks of the same parallel operator instances
				List<DeployGraphState.OperatorTask> operatorTaskList = new ArrayList<>(jobVertex.getParallelism());
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

					Host host = hosts.get(slot.getTaskManagerLocation().getResourceID());
					if (host == null) {
						host = new Host(slot.getTaskManagerLocation());
						hosts.put(slot.getTaskManagerLocation().getResourceID(), host);
					}
					OperatorTask operatorTask = new OperatorTask(slot, host);
					operatorTaskList.add(operatorTask);
				}
				for (OperatorID operatorID : jobVertex.getOperatorIDs()) {
					operatorTaskListMap.put(operatorID, operatorTaskList);
				}
			}
		}

		@Override
		public Host[] getHosts() {
			return hosts.values().toArray(new Host[0]);
		}

		@Override
		public OperatorTask getTask(OperatorID operatorID, int offset) {
			return operatorTaskListMap.get(operatorID).get(offset);
		}
	}

}

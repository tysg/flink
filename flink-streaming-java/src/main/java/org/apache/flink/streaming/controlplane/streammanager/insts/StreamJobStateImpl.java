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

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.controlplane.reconfigure.JobGraphUpdater;
import org.apache.flink.streaming.controlplane.rescale.StreamJobGraphRescaler;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class StreamJobStateImpl implements StreamJobState {

	private final static int COMMITTED = 1;
	private final static int STAGED = 0;

	private final OperatorGraph operatorGraph;
	private final DeploymentGraph deploymentGraph;

	private final JobGraphUpdater jobGraphUpdater;

	private final ClassLoader userCodeLoader;

	private final AtomicInteger stateOfUpdate = new AtomicInteger(COMMITTED);

	private ControlPolicy currentWaitingController;


	private StreamJobStateImpl(OperatorGraph operatorGraph, DeploymentGraph deploymentGraph, ClassLoader userLoader) {
		this.operatorGraph = operatorGraph;
		this.deploymentGraph = deploymentGraph;
		this.userCodeLoader = userLoader;

		this.jobGraphUpdater = new JobGraphUpdater(
			new StreamJobGraphRescaler(operatorGraph.jobGraph, userCodeLoader),
			this);
	}

	public static StreamJobStateImpl createFromGraph(JobGraph jobGraph, ExecutionGraph executionGraph) {
		ClassLoader userLoader = executionGraph.getUserClassLoader();
		return new StreamJobStateImpl(new OperatorGraph(jobGraph), new DeploymentGraph(executionGraph), userLoader);
	}

	@Override
	public JobGraph getJobGraph() {
		return operatorGraph.jobGraph;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userCodeLoader;
	}

	public Function getUserFunction(OperatorID operatorID) throws Exception {
		StreamConfig config = findStreamConfig(operatorID);
		return ((SimpleUdfStreamOperatorFactory<?>) config.getStreamOperatorFactory(this.userCodeLoader)).getUserFunction();
	}

	public <OUT> JobVertexID updateOperator(OperatorID operatorID,
											StreamOperatorFactory<OUT> operatorFactory,
											ControlPolicy waitingController) throws Exception {
		setStateUpdatingFlag(waitingController);
		return jobGraphUpdater.updateOperator(operatorID, operatorFactory);
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


	@Override
	public List<Integer> getKeyStateAllocation(OperatorID operatorID) throws Exception {
		Map<OperatorID, Integer> map = new HashMap<>();
		StreamConfig config = findStreamConfig(operatorID);

		List<Integer> res = new ArrayList<>(config.getNumberOfInputs());
		List<StreamEdge> inEdges = config.getInPhysicalEdges(this.userCodeLoader);

		return res;
	}

	@Override
	public List<List<Integer>> getKeyMapping(OperatorID operatorID) throws Exception {
		Map<OperatorID, Integer> map = new HashMap<>();
		StreamConfig config = findStreamConfig(operatorID);

		List<List<Integer>> res = new ArrayList<>(config.getNumberOfOutputs());
		List<StreamEdge> outEdges = config.getOutEdges(this.userCodeLoader);
		for(StreamEdge edge: outEdges){

		}

		return res;
	}

	private void getKeyMessage(StreamEdge streamEdge){
		StreamPartitioner<?> partitioner = streamEdge.getPartitioner();




	}

	@Override
	public int getParallelism(OperatorID operatorID) {
		Map<OperatorID, Integer> map = new HashMap<>();
		for(JobVertex vertex: this.operatorGraph.jobGraph.getVertices()){
			int parallelism = vertex.getParallelism();
			for(OperatorID id: vertex.getOperatorIDs()){
				map.put(id, parallelism);
			}
		}
		return map.get(operatorID);
	}

	@Override
	public List<Host> getHosts() {
		return null;
	}

	@Override
	public OperatorTask getTask(OperatorID operatorID, int offset) {
		return null;
	}

	private StreamConfig findStreamConfig(OperatorID operatorID) throws Exception {
		for (JobVertex vertex : this.operatorGraph.jobGraph.getVertices()) {
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

	public static class OperatorGraph {
		/* contains topology of this stream job */
		private final JobGraph jobGraph;

		OperatorGraph(JobGraph jobGraph) {
			this.jobGraph = Preconditions.checkNotNull(jobGraph);
		}

	}

	public static class DeploymentGraph {
		/* contains topology of this stream job */
		private final ExecutionGraph executionGraph;

		DeploymentGraph(ExecutionGraph executionGraph) {
			this.executionGraph = Preconditions.checkNotNull(executionGraph);
		}

	}


}

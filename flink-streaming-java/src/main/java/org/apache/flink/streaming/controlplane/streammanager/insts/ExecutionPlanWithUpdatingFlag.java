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
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import javax.annotation.Nullable;

/**
 * The state of stream manager mainly contains the following information:
 * 1. topology: the topology of the whole dataflow,
 * 2. key mappings: mappings of upstream key groups to downstream tasks,
 * 3. key state allocation: key groups allocation among tasks in the same stage,
 * 4. user defined function:user defined execution logic of each task,
 * 5. nThreads: num-ber of threads owned by each task,
 * 6. task location: location of one task in cluster.
 * <p>
 * G(V,E) is a graph with a set of vertices V connected by a set of edges E.
 * G(V,E) describes the operator level abstraction.
 * V contains the execution logic configurations: user defined function and key state allocation.
 * E provides the connectivity information of different vertices, the main information is keymappings.
 * <p>
 * D(H,T) is the deployment of tasks of the streaming job on the cluster, it describes task level abstraction.
 * H represents the hosts in the cluster, each host has a certain number of CPU and memory resources.
 * T is the set of tasks, the main information in T is: number of threads owned by each task and task location.
 */
public interface ExecutionPlanWithUpdatingFlag extends ExecutionPlan {
	/**
	 * get job graph from stream manager (the state of stream manager)
	 *
	 * @return current job graph of stream manager
	 */
//	OperatorGraphState getOperatorGraph();

	/**
	 * get user class loader of job graph (the state of stream manager)
	 *
	 * @return current user class loader of job graph
	 */
//	ClassLoader getUserClassLoader();

//	/**
//	 * @param operatorID      the id of target operarir
//	 * @param operatorFactory the new stream operator factory
//	 * @param <OUT>           Output type of StreamOperatorFactory
//	 * @return the id of updated job vertex
//	 * @throws Exception
//	 */
//	@Internal
//	<OUT> JobVertexID updateOperator(OperatorID operatorID,
//									 StreamOperatorFactory<OUT> operatorFactory,
//									 @Nullable ControlPolicy waitingController) throws Exception;

	@Internal
	void setStateUpdatingFlag(@Nullable ControlPolicy waitingController) throws Exception;

	/**
	 * Notify that current state update is finished.
	 * This could only be invoke once
	 *
	 * @throws Exception
	 */
	@Internal
	void notifyUpdateFinished(Throwable throwable) throws Exception;
}

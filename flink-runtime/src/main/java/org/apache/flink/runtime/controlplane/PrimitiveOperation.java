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

package org.apache.flink.runtime.controlplane;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The low level primitive operations.
 * <p>
 * - Prepare execution plan: the main goal is to convert the abstracted execution plan in StreamManger to
 * the execution plan maintained by each streaming system.
 * - Synchronize tasks: Synchronize the whole dataflow of the streaming job, temporarily pause the affected tasks.
 * - Deploy/Cancel tasks: request or free the resources request from the cluster.
 * - Update key mapping: update key mappings of affectedtasks.
 * - Update key state: Update key state of affected tasks
 */
public interface PrimitiveOperation {

	/**
	 * Analyze the difference between current physical execution plan and passed abstract execution plan.
	 *
	 * @param jobExecutionPlan the abstract execution plan which is maintained by some one
	 * @return
	 */
	CompletableFuture<Void> prepareExecutionPlan(StreamJobExecutionPlan jobExecutionPlan);

	/**
	 * Synchronize the whole dataflow of the streaming job, temporarily pause the affected tasks.
	 *
	 * @param taskList The list of task id, each id is a tuple which the first element is operator id and the second element is offset
	 * @return
	 */
	CompletableFuture<Void> synchronizePauseTasks(List<Tuple2<Integer, Integer>> taskList);

	/**
	 * Resume the paused tasks by synchronize.
	 *
	 * In implementation, since we use MailBoxProcessor to pause tasks,
	 * to make this resume methods make sense, the task's MailBoxProcessor should not be changed.
	 *
	 * @param taskList The list of task id, each id is a tuple which the first element is operator id and the second element is offset
	 * @return
	 */
	CompletableFuture<Void> resumeTasks(List<Tuple2<Integer, Integer>> taskList);

	/**
	 * Request the resources request from the cluster.
	 *
	 * @param operatorID the operator id of this operator
	 * @param offset     represent which parallel instance of this operator, -1 means all parallel instance
	 * @return
	 */
	CompletableFuture<Void> deployTasks(int operatorID, int offset);

	/**
	 * Free the resources request from the cluster.
	 *
	 * @param operatorID the operator id of this operator
	 * @param offset     represent which parallel instance of this operator, -1 means all parallel instance
	 * @return
	 */
	CompletableFuture<Void> cancelTasks(int operatorID, int offset);

	/**
	 * Update key mappings between srcOp and destOp
	 *
	 * @param srcOpID the operator id of source operator
	 * @param destOpID the operator id of destination operator
	 * @return
	 */
	CompletableFuture<Void> updateMapping(int srcOpID, int destOpID);

	/**
	 * update the key state in destination operator
	 *
	 * @param keySenderID the id of which operator send keys to destination operator
	 * @param operatorID the id of operator that need to update state
	 * @param offset  the sub-operator offset of update stated needed operator
	 * @return
	 */
	CompletableFuture<Void> updateState(int keySenderID, int operatorID, int offset);

	/**
	 * @param vertexID the operator id of this operator
	 * @param offset   represent which parallel instance of this operator, -1 means all parallel instance
	 * @return
	 */
	CompletableFuture<Acknowledge> updateFunction(int vertexID, int offset);

	/**
	 * Deprecated since we need a general primitive operation, use {@code PrimitiveOperation::updateFunction(int vertexID, int offset)},
	 * The JobGraph should be not visitable for control policy writer.
	 *
	 * @param jobGraph
	 * @param targetVertexID
	 * @param operatorID
	 * @return
	 */
	@Deprecated
	CompletableFuture<Acknowledge> updateFunction(@Nullable JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID);

}

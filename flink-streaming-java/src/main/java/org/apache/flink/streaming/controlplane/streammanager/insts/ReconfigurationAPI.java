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

import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.rescale.JobRescaleAction;
import org.apache.flink.runtime.rescale.reconfigure.AbstractCoordinator;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * This interface defined some instruction which could be called by several control policy user defined model
 * to update the stream job execution plan which is maintained by stream manager.
 */
public interface ReconfigurationAPI {

	/**
	 * Get the state of stream job managed by this stream manager
	 *
	 * @return the stream job state maintained in some place (e.g. {@link org.apache.flink.streaming.controlplane.streammanager.StreamManager})
	 */
	StreamJobExecutionPlan getJobExecutionPlan();

	@Deprecated
	void rescaleStreamJob(JobRescaleAction.RescaleParamsWrapper wrapper);

	void rescale(int operatorID, int newParallelism, Map<Integer, List<Integer>> keyStateAllocation, ControlPolicy waitingController);

	void rebalance(int operatorID, Map<Integer, List<Integer>> keyStateAllocation, boolean stateful, ControlPolicy waitingController);

	/**
	 * Use to notify job master that some operator inside job vertex changed,
	 * Thus the corresponding executor could substitute new operator from the original one.
	 *
	 * todo stream operator factory belongs to flink, should we decouple it?
	 *
	 * @param operatorID the id of changed operator
	 * @param function the new operator UDF, noted this only suitable for UDFOperator
	 */
	void reconfigureUserFunction(int operatorID, Object function, ControlPolicy waitingController);

	void noOp(int operatorID, ControlPolicy waitingController);

	default void callCustomizeOperations(
		Function<PrimitiveOperation<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>>, CompletableFuture<?>> operationCaller){

	}
}

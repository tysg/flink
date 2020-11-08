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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobAbstraction;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ReconfigurableStreamJobExecutionPlan implements StreamJobAbstraction, StreamJobReconfigurable {

	private final static int COMMITTED = 1;
	private final static int STAGED = 0;

	private final AtomicInteger stateOfUpdate = new AtomicInteger(COMMITTED);
	private ControlPolicy currentWaitingController;

	private final StreamJobAbstraction streamJobAbstractionDelegate;

	public ReconfigurableStreamJobExecutionPlan(StreamJobAbstraction streamJobAbstractionDelegate) {
		this.streamJobAbstractionDelegate = streamJobAbstractionDelegate;
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
	public void notifyUpdateFinished(Integer jobVertexID) throws Exception {
		if (stateOfUpdate.compareAndSet(STAGED, COMMITTED)) {
			if (currentWaitingController != null) {
				currentWaitingController.onChangeCompleted(jobVertexID);
			}
			return;
		}
		throw new Exception("There is not any state updating");
	}

	// delegate methods
	@Override
	public Host[] getHosts() {
		return streamJobAbstractionDelegate.getHosts();
	}

	@Override
	public OperatorTask getTask(Integer operatorID, int offset) {
		return streamJobAbstractionDelegate.getTask(operatorID, offset);
	}

	@Override
	public Function getUserFunction(Integer operatorID) throws Exception {
		return streamJobAbstractionDelegate.getUserFunction(operatorID);
	}

	@Override
	public Map<Integer, List<List<Integer>>> getKeyStateAllocation(Integer operatorID) throws Exception {
		return streamJobAbstractionDelegate.getKeyStateAllocation(operatorID);
	}

	@Override
	public Map<Integer, List<List<Integer>>> getKeyMapping(Integer operatorID) throws Exception {
		return streamJobAbstractionDelegate.getKeyMapping(operatorID);
	}

	@Override
	public int getParallelism(Integer operatorID) {
		return streamJobAbstractionDelegate.getParallelism(operatorID);
	}

	@Override
	public Iterator<OperatorDescriptor> getAllOperatorDescriptor() {
		return streamJobAbstractionDelegate.getAllOperatorDescriptor();
	}
}

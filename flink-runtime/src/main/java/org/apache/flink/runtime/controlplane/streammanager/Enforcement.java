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

package org.apache.flink.runtime.controlplane.streammanager;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * General enforcements
 *
 * prepareExecutionPlan(State)
 * synchronizeTasks({})
 * deployTasks()
 * updateMapping()
 * updateState()
 */
public interface Enforcement {

	void prepareExecutionPlan();

	CompletableFuture<Void> synchronizeTasks(Collection<JobVertexID> jobVertexIDS);

	void deployTasks();

	void updateMapping();

	void updateState();

	void updateFunction(@Nullable JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID);

	@FunctionalInterface
	interface EnforcementCaller {
		void call(Enforcement enforcement);
	}

}

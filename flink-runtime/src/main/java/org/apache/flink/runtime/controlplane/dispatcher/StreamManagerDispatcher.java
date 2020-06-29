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

package org.apache.flink.runtime.controlplane.dispatcher;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.runtime.controlplane.webmonitor.StreamManagerDispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.function.CheckedSupplier;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for the StreamManagerDispatcher component. The StreamManagerDispatcher
 * component is responsible for receiving job submissions, persisting them, spawning
 * StreamManagers to control the JobManagers and to recover them in case of a master
 * failure.
 */
public abstract class StreamManagerDispatcher
	extends PermanentlyFencedRpcEndpoint<StreamManagerDispatcherId>
	implements StreamManagerDispatcherGateway {

	public static final String DISPATCHER_NAME = "stream-manager-dispatcher";
	private StreamManagerRunnerFactory streamManagerRunnerFactory;

	protected StreamManagerDispatcher(RpcService rpcService,
									  String endpointId,
									  StreamManagerDispatcherId fencingToken,
									  StreamManagerRunnerFactory factory) {
		super(rpcService, endpointId, fencingToken);
		this.streamManagerRunnerFactory = factory;
	}


	private void startDispatcherService() {

	}

	private CompletableFuture<StreamManagerRunner> startStreamManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();

		return CompletableFuture.supplyAsync(
			CheckedSupplier.unchecked(() ->
				streamManagerRunnerFactory.createStreamManagerRunner(
					jobGraph,
					null,
					rpcService,
					null,
					null,
					null
				)
			), rpcService.getExecutor());
	}

	@Override
	public void close() throws Exception {

	}

	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId){
		throw new NotImplementedException("may not need to remove jobs");
	}
}

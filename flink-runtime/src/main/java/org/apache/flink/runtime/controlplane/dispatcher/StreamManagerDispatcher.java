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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.runtime.controlplane.webmonitor.StreamManagerDispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.FunctionUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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

	protected final CompletableFuture<ApplicationStatus> shutDownFuture;


	protected StreamManagerDispatcher(RpcService rpcService,
									  String endpointId,
									  StreamManagerDispatcherId fencingToken,
									  StreamManagerRunnerFactory factory) {
		super(rpcService, endpointId, fencingToken);
		this.streamManagerRunnerFactory = factory;
		shutDownFuture = new CompletableFuture<>();
	}


	private void startDispatcherService() {

	}

	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		// SMD refers to StreamManagerDispatcher
		log.info("[SMD] Received JobGraph submission {} ({})", jobGraph.getJobID(), jobGraph.getName());
		// TODO: deal with duplicates and partial resource
		return internalSubmitJob(jobGraph);
	}

	private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
		log.info("[SMD] Submitting job {} ({})", jobGraph.getJobID(), jobGraph.getName());
		// TODO: waitForTerminatingStreamManager
		final CompletableFuture<Acknowledge> submitDirectly = persistAndRunJob(jobGraph)
				.thenApply(ignored -> Acknowledge.get());
		return submitDirectly.handleAsync((acknowledge, throwable) -> {
			if (throwable != null) {
				//TODO: cleanup data

				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
				log.error("Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
				throw new CompletionException(
						new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
			} else {
				return acknowledge;
			}
		}, getRpcService().getExecutor());
	}

	private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) {
		// TODO: jobGraphWriter
		final CompletableFuture<Void> runSMFuture = runStreamManager(jobGraph);
		return runSMFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

				log.error("Failed to run stream manager.", strippedThrowable);
				// jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

	private CompletableFuture<Void> runStreamManager(JobGraph jobGraph) {

		final CompletableFuture<StreamManagerRunner> streamManagerRunnerFuture = createStreamManagerRunner(jobGraph);

		return streamManagerRunnerFuture
				.thenApply(FunctionUtils.uncheckedFunction(this::startStreamManagerRunner))
				.thenApply(FunctionUtils.nullFn())
				.whenCompleteAsync(
						(ignored, throwable) -> {
						},
						getMainThreadExecutor());

	}


	private CompletableFuture<StreamManagerRunner> createStreamManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();
		return CompletableFuture.supplyAsync(
				CheckedSupplier.unchecked(() ->
						streamManagerRunnerFactory.createStreamManagerRunner(
								jobGraph,
								null, //configuration,
								rpcService,
								null, //highAvailabilityServices,
								null, //heartbeatServices,
								null //fatalErrorHandler
						)),
				rpcService.getExecutor());
	}

	private StreamManagerRunner startStreamManagerRunner(StreamManagerRunner streamManagerRunner) throws Exception {
		streamManagerRunner.start();
		return streamManagerRunner;
	}

	@Override
	public void close() throws Exception {

	}

	//------------------------------------------------------
	// Getters
	//------------------------------------------------------

	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId){
		throw new ControlPlaneNotImplementedException("may not need to remove jobs");
	}
}

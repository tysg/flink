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

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerId;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerService;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.OptionalConsumer;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author trx
 * StreamManager implementation.
 *
 * TODO:
 * 1. decide other fields
 * 2. initialize other fields
 */
public class StreamManager extends FencedRpcEndpoint<StreamManagerId> implements StreamManagerGateway, StreamManagerService {

    /** Default names for Flink's distributed components. */
    public static final String Stream_Manager_NAME = "streammanager";

    private final StreamManagerConfiguration streamManagerConfiguration;

    private final ResourceID resourceId;

    private final JobGraph jobGraph;

    private final Time rpcTimeout;

    private final HighAvailabilityServices highAvailabilityServices;

    private final FatalErrorHandler fatalErrorHandler;

    private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;

    /*

    // --------- JobManager --------

    private final LeaderRetrievalService jobManagerLeaderRetriever;

    // -------- Mutable fields ---------

    private HeartbeatManager<Void, Void> jobManagerHeartbeatManager;

    TODO: other connection and listener related

    */

    // ------------------------------------------------------------------------

    public StreamManager(RpcService rpcService,
						 StreamManagerConfiguration streamManagerConfiguration,
						 ResourceID resourceId,
						 JobGraph jobGraph,
						 HighAvailabilityServices highAvailabilityService,
						 LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
						 FatalErrorHandler fatalErrorHandler) throws Exception{
        super(rpcService, AkkaRpcServiceUtils.createRandomName(Stream_Manager_NAME), null);

        this.streamManagerConfiguration = checkNotNull(streamManagerConfiguration);
        this.resourceId = checkNotNull(resourceId);
        this.jobGraph = checkNotNull(jobGraph);
        this.rpcTimeout = streamManagerConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.dispatcherGatewayRetriever = checkNotNull(dispatcherGatewayRetriever);

        final String jobName = jobGraph.getName();
        final JobID jid = jobGraph.getJobID();

		log.debug("Initializing sm for job {} ({})", jobName, jid);
		log.info("Initializing sm for job {} ({})", jobName, jid);

        /*
        TODO: initialize other fields
         */


		OptionalConsumer<DispatcherGateway> optLeaderConsumer = OptionalConsumer.of(this.dispatcherGatewayRetriever.getNow());

		optLeaderConsumer.ifPresent(
			gateway -> {
				try {
					log.info("connect successfully");
					gateway.submitJob(jobGraph, Time.seconds(10));
				} catch (Exception e) {
					log.error("Error while invoking runtime dispatcher RMI.", e);
				}
			}
		).ifNotPresent(
			() ->
				log.error("Error while connecting runtime dispatcher."));
    }

    /**
     * Start the StreamManager service with the given {@link StreamManagerId}.
     *
     * @param newStreamManagerId to start the service with
     * @return Future which is completed once the StreamManager service has been started
     * @throws Exception if the StreamManager service could not be started
     */
    @Override
    public CompletableFuture<Acknowledge> start(StreamManagerId newStreamManagerId) throws Exception {
        // make sure we receive RPC and async calls
        start();

        return callAsyncWithoutFencing(() -> startStreamManagement(newStreamManagerId), RpcUtils.INF_TIMEOUT);
    }

    /**
     * Suspend the StreamManager service. This means that the service will stop to react
     * to messages.
     *
     * @param cause for the suspension
     * @return Future which is completed once the StreamManager service has been suspended
     */
    @Override
    public CompletableFuture<Acknowledge> suspend(Exception cause) {
        CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(
                () -> suspendManagement(cause),
                RpcUtils.INF_TIMEOUT);
        return suspendFuture.whenComplete(((acknowledge, throwable) -> stop()));
    }

    // ------------------------------------------------------------------------
    //  RPC methods
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<RegistrationResponse> registerJobManager(
            final JobMasterId jobMasterId,
            final ResourceID jobManagerResourceId,
            final String jobManagerAddress,
            final JobID jobId,
            final Time timeout) {

        checkNotNull(jobMasterId);
        checkNotNull(jobManagerResourceId);
        checkNotNull(jobManagerAddress);
        checkNotNull(jobId);
        return null; // TODO: to be implement
    }

    @Override
    public void disconnectJobMaster(JobMasterId jobMasterId, Exception cause) {
    }


    //----------------------------------------------------------------------------------------------
    // Internal methods
    //----------------------------------------------------------------------------------------------

    private Acknowledge startStreamManagement(StreamManagerId newStreamManagerId) throws Exception {

        validateRunsInMainThread();

        checkNotNull(newStreamManagerId, "The new StreamManagerId must not be null");

        return Acknowledge.get();
    }

    /**
     * Suspending stream manager, (cancel the job, to be consider), and other communication with other components
     * will be disposed.
     * @param cause The reason of why this stream manger been suspended.
     */
    private Acknowledge suspendManagement(final Exception cause) {
        validateRunsInMainThread();

        if (getFencingToken() == null) {
            log.debug("Stream Management has already benn suspended or shutdown.");
            return Acknowledge.get();
        }

        // not leader anymore --> set the StreamManagerId to null
        setFencingToken(null);

        // TODO:
        // closeJobManagerConnection(cause);

        // stop other services

        return Acknowledge.get();
    }

    //----------------------------------------------------------------------------------------------
    // Service methods
    //----------------------------------------------------------------------------------------------

    /**
     * Get the {@link StreamManagerGateway} belonging to this service.
     *
     * @return StreamManagerGateway belonging to this service
     */
    @Override
    public StreamManager getGateway() {
        return getSelfGateway(StreamManager.class);
    }
}

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.CompletableFuture;

public class TestingStreamManager extends StreamManager {

	private final CompletableFuture<Acknowledge> isRegisterJob;

	public TestingStreamManager(RpcService rpcService,
								StreamManagerConfiguration streamManagerConfiguration,
								ResourceID resourceId,
								JobGraph jobGraph,
								HighAvailabilityServices highAvailabilityService,
								JobLeaderIdService jobLeaderIdService,
								LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
								FatalErrorHandler fatalErrorHandler,
								CompletableFuture<Acknowledge> isRegisterJob) throws Exception {
		super(rpcService,
			streamManagerConfiguration,
			resourceId,
			jobGraph,
			highAvailabilityService,
			jobLeaderIdService,
			dispatcherGatewayRetriever,
			fatalErrorHandler);
		this.isRegisterJob = isRegisterJob;
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(JobMasterId jobMasterId, ResourceID jobManagerResourceId, String jobManagerAddress, JobID jobId, Time timeout) {
		isRegisterJob.complete(Acknowledge.get());
		return super.registerJobManager(jobMasterId, jobManagerResourceId, jobManagerAddress, jobId, timeout);
	}
}

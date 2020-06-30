package org.apache.flink.runtime.controlplane.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class SessionStreamManagerDispatcher extends StreamManagerDispatcher {

	SessionStreamManagerDispatcher(RpcService rpcService,
								   String endpointId,
								   StreamManagerDispatcherId fencingToken,
								   StreamManagerRunnerFactory factory) {
		super(rpcService, endpointId, fencingToken, factory);
	}

	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
		return null;
	}
}

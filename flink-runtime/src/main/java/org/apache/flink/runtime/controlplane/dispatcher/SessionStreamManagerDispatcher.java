package org.apache.flink.runtime.controlplane.dispatcher;

import akka.remote.Ack;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.FunctionUtils;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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

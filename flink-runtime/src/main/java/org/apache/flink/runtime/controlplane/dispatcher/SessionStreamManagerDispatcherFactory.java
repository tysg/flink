package org.apache.flink.runtime.controlplane.dispatcher;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.runtime.dispatcher.DefaultJobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

public enum SessionStreamManagerDispatcherFactory implements StreamManagerDispatcherFactory {
	INSTANCE;

	@Override
	public StreamManagerDispatcher createStreamManagerDispatcher(RpcService rpcService, StreamManagerDispatcherId fencingToken) throws Exception {
		return new SessionStreamManagerDispatcher(
			rpcService,
			getEndpointId(),
			fencingToken,
			new TempSMRunnerFactory()
		);
	}

	static class TempSMRunnerFactory implements StreamManagerRunnerFactory {

		@Override
		public StreamManagerRunner createStreamManagerRunner(JobGraph jobGraph,
														  Configuration configuration,
														  RpcService rpcService,
														  HighAvailabilityServices highAvailabilityServices,
														  HeartbeatServices heartbeatServices,
														  FatalErrorHandler fatalErrorHandler) throws Exception {
			throw new NotImplementedException("temp solution, Runxin does not implement this currently!");
		}
	}
}

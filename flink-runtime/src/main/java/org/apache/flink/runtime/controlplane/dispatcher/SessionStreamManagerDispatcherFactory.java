package org.apache.flink.runtime.controlplane.dispatcher;

import org.apache.flink.runtime.rpc.RpcService;

public enum SessionStreamManagerDispatcherFactory implements StreamManagerDispatcherFactory {
	INSTANCE;

	@Override
	public StreamManagerDispatcher createStreamManagerDispatcher(RpcService rpcService, StreamManagerDispatcherId fencingToken) throws Exception {
		return new SessionStreamManagerDispatcher(
			rpcService,
			getEndpointId(),
			fencingToken,
			DefaultStreamManagerRunnerFactory.INSTANCE);
	}
}

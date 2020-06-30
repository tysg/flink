package org.apache.flink.runtime.controlplane.dispatcher.runner;

import org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcher;
import org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherFactory;
import org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherId;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;


public class DefaultStreamManagerDispatcherGatewayServiceFactory implements AbstractStreamManagerDispatcherLeaderProcess.DispatcherGatewayServiceFactory {

	private StreamManagerDispatcherFactory dispatcherFactory;
	private RpcService rpcService;
	private PartialDispatcherServices partialDispatcherServices;

	DefaultStreamManagerDispatcherGatewayServiceFactory(
		StreamManagerDispatcherFactory dispatcherFactory,
		RpcService rpcService,
		PartialDispatcherServices partialDispatcherServices) {

		this.dispatcherFactory = dispatcherFactory;
		this.rpcService = rpcService;
		this.partialDispatcherServices = partialDispatcherServices;
	}


	@Override
	public AbstractStreamManagerDispatcherLeaderProcess.DispatcherGatewayService create(
		StreamManagerDispatcherId fencingToken,
		JobGraphWriter jobGraphWriter) {

		final StreamManagerDispatcher dispatcher;
		try {
			dispatcher = dispatcherFactory.createStreamManagerDispatcher(
				rpcService,
				fencingToken);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
		}

		dispatcher.start();

		return DefaultStreamManagerDispatcherGatewayService.from(dispatcher);
	}


}

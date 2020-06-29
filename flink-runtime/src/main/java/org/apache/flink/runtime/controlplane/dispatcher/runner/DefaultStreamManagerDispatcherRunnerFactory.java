package org.apache.flink.runtime.controlplane.dispatcher.runner;

import org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.runner.*;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

public class DefaultStreamManagerDispatcherRunnerFactory implements StreamManagerDispatcherRunnerFactory {

	private final DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory;

	private DefaultStreamManagerDispatcherRunnerFactory(DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory) {
		this.dispatcherLeaderProcessFactoryFactory = dispatcherLeaderProcessFactoryFactory;
	}

	public static DefaultStreamManagerDispatcherRunnerFactory createSessionRunner(StreamManagerDispatcherFactory dispatcherFactory) {
		return new DefaultStreamManagerDispatcherRunnerFactory(
			SessionDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
	}

	public static DefaultStreamManagerDispatcherRunnerFactory createJobRunner(JobGraphRetriever jobGraphRetriever) {
		return null;
//		return new DefaultDispatcherRunnerFactory(
//			JobDispatcherLeaderProcessFactoryFactory.create(jobGraphRetriever));
	}

	@Override
	public StreamManagerDispatcherRunner createStreamManagerDispatcherRunner(
		LeaderElectionService leaderElectionService,
		FatalErrorHandler fatalErrorHandler,
		JobGraphStoreFactory jobGraphStoreFactory,
		Executor ioExecutor, RpcService rpcService,
		PartialDispatcherServices partialDispatcherServices) throws Exception {

		final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactoryFactory.createFactory(
			jobGraphStoreFactory,
			ioExecutor,
			rpcService,
			partialDispatcherServices,
			fatalErrorHandler);

		return DefaultStreamManagerDispatcherRunner.create(
			leaderElectionService,
			fatalErrorHandler,
			dispatcherLeaderProcessFactory);

	}
}

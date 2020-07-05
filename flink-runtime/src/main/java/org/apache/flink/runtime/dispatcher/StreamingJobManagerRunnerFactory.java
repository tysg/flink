package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerAddress;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.*;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.StreamingJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;

public class StreamingJobManagerRunnerFactory implements JobManagerRunnerFactory {

	private StreamManagerAddress streamManagerAddress;

	public StreamingJobManagerRunnerFactory(StreamManagerAddress streamManagerAddress){
		this.streamManagerAddress = streamManagerAddress;
	}

	@Override
	public JobManagerRunner createJobManagerRunner(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		JobManagerSharedServices jobManagerServices,
		JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
		FatalErrorHandler fatalErrorHandler) throws Exception {

		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		final SlotPoolFactory slotPoolFactory = DefaultSlotPoolFactory.fromConfiguration(configuration);
		final SchedulerFactory schedulerFactory = DefaultSchedulerFactory.fromConfiguration(configuration);
		final SchedulerNGFactory schedulerNGFactory = SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration, jobManagerServices.getRestartStrategyFactory());
		final ShuffleMaster<?> shuffleMaster = ShuffleServiceLoader.loadShuffleServiceFactory(configuration).createShuffleMaster(configuration);

		final JobMasterServiceFactory jobMasterFactory = new DefaultJobMasterServiceFactory(
			jobMasterConfiguration,
			slotPoolFactory,
			schedulerFactory,
			rpcService,
			highAvailabilityServices,
			jobManagerServices,
			heartbeatServices,
			jobManagerJobMetricGroupFactory,
			fatalErrorHandler,
			schedulerNGFactory,
			shuffleMaster);

		return new JobManagerRunnerImpl(
			jobGraph,
			new StreamingJobMasterServiceFactory(jobMasterFactory, streamManagerAddress),
			highAvailabilityServices,
			jobManagerServices.getLibraryCacheManager(),
			jobManagerServices.getScheduledExecutorService(),
			fatalErrorHandler);
	}
}

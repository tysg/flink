package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.controlplane.streammanager.StreamManagerAddress;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.concurrent.Executor;

public class StreamingJobManagerRunner extends JobManagerRunnerImpl {

	private StreamManagerAddress streamManagerAddress;

	/**
	 * Exceptions that occur while creating the JobManager or JobManagerRunnerImpl are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 *
	 * @throws Exception Thrown if the runner cannot be set up, because either one of the
	 *                   required services could not be started, or the Job could not be initialized.
	 */
	public StreamingJobManagerRunner(
		JobGraph jobGraph,
		JobMasterServiceFactory jobMasterFactory,
		HighAvailabilityServices haServices,
		LibraryCacheManager libraryCacheManager,
		Executor executor,
		FatalErrorHandler fatalErrorHandler,
		StreamManagerAddress streamManagerAddress) throws Exception {
		super(jobGraph,
			jobMasterFactory,
			haServices,
			libraryCacheManager,
			executor,
			fatalErrorHandler);
		this.streamManagerAddress = streamManagerAddress;
	}
}

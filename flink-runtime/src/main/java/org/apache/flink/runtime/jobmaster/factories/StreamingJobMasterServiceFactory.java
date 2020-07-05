package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.controlplane.streammanager.StreamManagerAddress;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterService;

public class StreamingJobMasterServiceFactory implements JobMasterServiceFactory {

	private final JobMasterServiceFactory factory;
	private final StreamManagerAddress streamManagerAddress;

	public StreamingJobMasterServiceFactory(JobMasterServiceFactory factory, StreamManagerAddress streamManagerAddress) {
		this.factory = factory;
		this.streamManagerAddress = streamManagerAddress;
	}

	@Override
	public JobMasterService createJobMasterService(
		JobGraph jobGraph,
		OnCompletionActions jobCompletionActions,
		ClassLoader userCodeClassloader) throws Exception {
		JobMasterService jobMasterService = factory.createJobMasterService(jobGraph, jobCompletionActions, userCodeClassloader);
		if (jobMasterService instanceof JobMaster) {
			((JobMaster) jobMasterService).setStreamManagerAddress(streamManagerAddress);
		}
		return jobMasterService;
	}
}

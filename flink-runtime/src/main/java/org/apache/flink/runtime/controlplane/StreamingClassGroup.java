package org.apache.flink.runtime.controlplane;

import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphRescaler;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphUpdater;

public final class StreamingClassGroup {

	private final Class<? extends StreamJobExecutionPlan> StreamJobExecutionPlanClass;
	private final Class<? extends JobGraphUpdater> JobGraphOperatorUpdateClass;
	private final Class<? extends JobGraphRescaler> JobGraphRescalerClass;

	public StreamingClassGroup(
		Class<? extends StreamJobExecutionPlan> streamJobExecutionPlanClass,
		Class<? extends JobGraphUpdater> jobGraphOperatorUpdateClass,
		Class<? extends JobGraphRescaler> jobGraphRescalerClass) {
		StreamJobExecutionPlanClass = streamJobExecutionPlanClass;
		JobGraphOperatorUpdateClass = jobGraphOperatorUpdateClass;
		JobGraphRescalerClass = jobGraphRescalerClass;
	}

	public Class<? extends StreamJobExecutionPlan> getStreamJobExecutionPlanClass() {
		return StreamJobExecutionPlanClass;
	}

	public Class<? extends JobGraphUpdater> getJobGraphOperatorUpdateClass() {
		return JobGraphOperatorUpdateClass;
	}

	public Class<? extends JobGraphRescaler> getJobGraphRescalerClass() {
		return JobGraphRescalerClass;
	}

}

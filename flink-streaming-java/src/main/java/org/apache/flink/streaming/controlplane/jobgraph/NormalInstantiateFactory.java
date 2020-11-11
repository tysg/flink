package org.apache.flink.streaming.controlplane.jobgraph;

import org.apache.flink.runtime.controlplane.StreamRelatedInstanceFactory;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphUpdater;
import org.apache.flink.streaming.controlplane.streammanager.insts.StreamJobExecutionPlanImpl;

public enum NormalInstantiateFactory implements StreamRelatedInstanceFactory {
	INSTANCE;

	@Override
	public StreamJobExecutionPlan createExecutionPlan(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader classLoader) {
		return new StreamJobExecutionPlanImpl(jobGraph, executionGraph, classLoader);
	}

	@Override
	public JobGraphUpdater createJobGraphUpdater(JobGraph jobGraph, ClassLoader classLoader) {
		return new StreamJobGraphUpdater(jobGraph, classLoader);
	}
}

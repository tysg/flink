package org.apache.flink.streaming.controlplane.jobgraph;

import org.apache.flink.runtime.controlplane.StreamRelatedInstanceFactory;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphUpdater;
import org.apache.flink.streaming.controlplane.streammanager.insts.ExecutionPlanImpl;

public enum NormalInstantiateFactory implements StreamRelatedInstanceFactory {
	INSTANCE;

	@Override
	public ExecutionPlan createExecutionPlan(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader classLoader) {
		return new ExecutionPlanImpl(jobGraph, executionGraph, classLoader);
	}

	@Override
	public JobGraphUpdater createJobGraphUpdater(JobGraph jobGraph, ClassLoader classLoader) {
		return new StreamJobGraphUpdater(jobGraph, classLoader);
	}
}

package org.apache.flink.runtime.controlplane;

import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphUpdater;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectInstanceFactory implements StreamRelatedInstanceFactory {

	private final StreamingClassGroup classGroup;

	public ReflectInstanceFactory(StreamingClassGroup classGroup){
		this.classGroup = classGroup;
	}

	@Override
	public StreamJobExecutionPlan createExecutionPlan(
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader classLoader) {
		try {
			Class<? extends StreamJobExecutionPlan> StreamJobExecutionPlanClass = classGroup.getStreamJobExecutionPlanClass();
			Constructor<? extends StreamJobExecutionPlan> constructor = StreamJobExecutionPlanClass
				.getConstructor(JobGraph.class, ExecutionGraph.class, ClassLoader.class);
			return constructor.newInstance(jobGraph, executionGraph, classLoader);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public JobGraphUpdater createJobGraphUpdater(
		JobGraph jobGraph,
		ClassLoader classLoader) {
		try {
			Class<? extends JobGraphUpdater> JobGraphOperatorUpdateClass  = classGroup.getJobGraphOperatorUpdateClass();
			Constructor<? extends JobGraphUpdater> constructor = JobGraphOperatorUpdateClass
				.getConstructor(JobGraph.class, ClassLoader.class);
			return constructor.newInstance(jobGraph, classLoader);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}

}

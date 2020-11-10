package org.apache.flink.runtime.controlplane;

import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public enum ReflectJobAbstractionInstanceFactory {
	INSTANCE;

	public StreamJobExecutionPlan reflectInstance(
		Class<? extends StreamJobExecutionPlan> StreamJobExecutionPlanClass,
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader classLoader) {
		try {
			Constructor<? extends StreamJobExecutionPlan> constructor = StreamJobExecutionPlanClass
				.getConstructor(JobGraph.class, ExecutionGraph.class, ClassLoader.class);
			return constructor.newInstance(jobGraph, executionGraph, classLoader);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}

}

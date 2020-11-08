package org.apache.flink.runtime.controlplane;

import org.apache.flink.runtime.controlplane.abstraction.StreamJobAbstraction;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public enum ReflectJobAbstractionInstanceFactory {
	instance;

	public StreamJobAbstraction reflectInstance(
		Class<? extends StreamJobAbstraction> StreamJobAbstractionClass,
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader classLoader){
		try {
			Constructor constructor = StreamJobAbstractionClass.getConstructor(JobGraph.class, ExecutionGraph.class, ClassLoader.class);
			return (StreamJobAbstraction) constructor.newInstance(jobGraph, executionGraph, classLoader);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}

}

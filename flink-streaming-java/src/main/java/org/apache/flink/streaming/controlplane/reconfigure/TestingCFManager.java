package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlOperatorFactory;
import org.apache.flink.streaming.controlplane.streammanager.insts.PrimitiveInstruction;
import org.apache.flink.streaming.controlplane.streammanager.insts.StreamJobState;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import javax.annotation.Nonnull;
import java.util.Map;

public class TestingCFManager extends ControlFunctionManager implements ControlPolicy {


	public TestingCFManager(PrimitiveInstruction primitiveInstruction) {
		super(primitiveInstruction);
	}

	@Override
	public void startControllerInternal() {
		System.out.println("Testing Control Function Manager starting...");

		StreamJobState jobState = primitiveInstruction.getStreamJobState();

		JobGraph currentJobGraph = jobState.getJobGraph();
		OperatorID secondOperatorId = findOperatorByName(currentJobGraph, jobState.getUserClassLoader(), "filter");

		if(secondOperatorId != null) {
			asyncRunAfter(10, () -> this.reconfigure(secondOperatorId, getFilterFunction(10)));
			asyncRunAfter(15, () -> this.reconfigure(secondOperatorId, getFilterFunction(20)));
			asyncRunAfter(25, () -> this.reconfigure(secondOperatorId, getFilterFunction(2)));
		}
	}

	@Override
	public void reconfigure(OperatorID operatorID, ControlFunction function) {
		System.out.println("Substitute `Control` Function...");
		ControlOperatorFactory<?, ?> operatorFactory = new ControlOperatorFactory<>(
			operatorID,
			primitiveInstruction.getStreamJobState().getJobGraph(),
			function);
		try {
			// since job graph is shared in stream manager and among its services, we don't need to pass it
			primitiveInstruction.changeOperator(operatorID, operatorFactory, this);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static ControlFunction getFilterFunction(int k) {
		return (ControlFunction) (ctx, input) -> {
			String inputWord = (String) input;
			System.out.println("now filter the words that has length smaller than " + k);
			if (inputWord.length() >= k) {
				ctx.setCurrentRes(inputWord);
			}
		};
	}


	private void asyncRunAfter(int seconds, Runnable runnable) {
		new Thread(() -> {
			try {
				Thread.sleep(seconds * 1000);
				runnable.run();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}).start();
	}


	private OperatorID findOperatorByName(JobGraph jobGraph,  ClassLoader classLoader, @Nonnull String name) {
		for (JobVertex vertex : jobGraph.getVertices()) {
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(classLoader);
			for (StreamConfig config : configMap.values()) {
				if (name.equals(config.getOperatorName())) {
					return config.getOperatorID();
				}
			}
		}
		return null;
	}

	@Override
	public void startControllers() {
		this.startControllerInternal();
	}

	@Override
	public void stopControllers() {

	}

	@Override
	public void onChangeCompleted(JobVertexID jobVertexID) {
		System.out.println("one operator function update is finished:"+jobVertexID);
	}
}

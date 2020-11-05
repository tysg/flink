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
import java.util.List;
import java.util.Map;

public class TestingCFManager extends ControlFunctionManager implements ControlPolicy {


	public TestingCFManager(PrimitiveInstruction primitiveInstruction) {
		super(primitiveInstruction);
	}

	@Override
	public void startControllerInternal() {
		System.out.println("Testing Control Function Manager starting...");

		StreamJobState jobState = getInstructionSet().getStreamJobState();

		JobGraph currentJobGraph = jobState.getJobGraph();
		OperatorID secondOperatorId = findOperatorByName(currentJobGraph, jobState.getUserClassLoader(), "Splitter");

		if(secondOperatorId != null) {
			asyncRunAfter(5, () -> this.getKeyStateMapping(
				findOperatorByName(currentJobGraph, jobState.getUserClassLoader(), "Splitter")));
			asyncRunAfter(5, () -> this.getKeyStateAllocation(
				findOperatorByName(currentJobGraph, jobState.getUserClassLoader(), "filte")));
			asyncRunAfter(10, () -> this.reconfigure(secondOperatorId, getFilterFunction(2)));
		}
	}

	private void getKeyStateMapping(OperatorID operatorID){
		try {
			Map<String, List<List<Integer>>> res = this.getInstructionSet().getStreamJobState().getKeyMapping(operatorID);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private void getKeyStateAllocation(OperatorID operatorID){
		try {
			Map<String, List<List<Integer>>> res = this.getInstructionSet().getStreamJobState().getKeyStateAllocation(operatorID);
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
		System.out.println("Testing Control Function Manager stopping...");
	}

	@Override
	public void onChangeCompleted(JobVertexID jobVertexID) {
		System.out.println(System.currentTimeMillis() + ":one operator function update is finished:"+jobVertexID);
	}
}

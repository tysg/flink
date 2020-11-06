package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.streammanager.insts.OperatorDescriptor;
import org.apache.flink.streaming.controlplane.streammanager.insts.OperatorGraphState;
import org.apache.flink.streaming.controlplane.streammanager.insts.PrimitiveInstruction;
import org.apache.flink.streaming.controlplane.streammanager.insts.StreamJobState;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import javax.annotation.Nonnull;
import java.util.Iterator;
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

		OperatorID secondOperatorId = findOperatorByName(jobState, "Splitter");

		if(secondOperatorId != null) {
			asyncRunAfter(5, () -> this.getKeyStateMapping(
				findOperatorByName(jobState, "Splitter")));
			asyncRunAfter(5, () -> this.getKeyStateAllocation(
				findOperatorByName(jobState, "filte")));
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


	private OperatorID findOperatorByName(OperatorGraphState operatorGraphState, @Nonnull String name) {
		for (Iterator<OperatorDescriptor> it = operatorGraphState.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			if(descriptor.getName().equals(name)){
				return descriptor.getOperatorID();
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

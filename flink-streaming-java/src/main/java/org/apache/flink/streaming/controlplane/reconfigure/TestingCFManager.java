package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.JobGraphConfig;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import javax.annotation.Nonnull;
import java.util.Iterator;

public class TestingCFManager extends ControlFunctionManager implements ControlPolicy {


	public TestingCFManager(ReconfigurationAPI reconfigurationAPI) {
		super(reconfigurationAPI);
	}

	@Override
	public void startControllerInternal() {
		System.out.println("Testing Control Function Manager starting...");

		StreamJobExecutionPlan jobState = getInstructionSet().getJobAbstraction();

		int secondOperatorId = findOperatorByName(jobState, "filte");

		if(secondOperatorId != -1) {
			asyncRunAfter(5, () -> this.getKeyStateMapping(
				findOperatorByName(jobState, "Splitter")));
			asyncRunAfter(5, () -> this.getKeyStateAllocation(
				findOperatorByName(jobState, "filte")));
			asyncRunAfter(10, () -> this.reconfigure(secondOperatorId, getFilterFunction(2)));
		}
	}

	private void getKeyStateMapping(int operatorID){
		try {
			this.getInstructionSet().getJobAbstraction().getKeyMapping(operatorID);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private void getKeyStateAllocation(int operatorID){
		try {
			this.getInstructionSet().getJobAbstraction().getKeyStateAllocation(operatorID);
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


	private int findOperatorByName(JobGraphConfig jobGraphConfig, @Nonnull String name) {
		for (Iterator<OperatorDescriptor> it = jobGraphConfig.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			if(descriptor.getName().equals(name)){
				return descriptor.getOperatorID();
			}
		}
		return -1;
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
	public void onChangeCompleted(Integer jobVertexID) {
		System.out.println(System.currentTimeMillis() + ":one operator function update is finished:"+jobVertexID);
	}

}

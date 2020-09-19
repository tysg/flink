package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.reconfigure.type.FunctionTypeStorage;
import org.apache.flink.streaming.controlplane.reconfigure.type.InMemoryFunctionStorge;

public class ControlFunctionManager implements ControlFunctionManagerService {

	private FunctionTypeStorage functionTypeStorage;
	protected JobGraph jobGraph;

	public ControlFunctionManager(JobGraph jobGraph){
		this.jobGraph = jobGraph;
		functionTypeStorage = new InMemoryFunctionStorge();
	}

	public void onJobStart(){
		System.out.println("Control Function Manager starting...");
	}


	/**
	 * we don't know how to register new function yet
	 * @param function target control function
	 */
	@Override
	public void registerFunction(ControlFunction function) {
		functionTypeStorage.addFunctionType(function.getClass());
	}

	@Override
	public void reconfigureFunction(OperatorID  operatorID, Class<? extends ControlFunction> type) {
		ControlFunction targetFunction = functionTypeStorage.getTargetFunction(type);
		System.out.println("Substitute `Control` Function...");
	}
}

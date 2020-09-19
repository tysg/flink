package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.reconfigure.operator.UpdatedOperator;
import org.apache.flink.streaming.controlplane.reconfigure.type.FunctionTypeStorage;
import org.apache.flink.streaming.controlplane.reconfigure.type.InMemoryFunctionStorge;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerService;

public class ControlFunctionManager implements ControlFunctionManagerService {

	private FunctionTypeStorage functionTypeStorage;
	protected final StreamManagerService streamManagerService;

	public ControlFunctionManager(StreamManagerService streamManagerService){
		this.streamManagerService = streamManagerService;
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
	public void reconfigure(OperatorID operatorID, UpdatedOperator operator) {
		System.out.println("Substitute `Control` Function...");
	}

}

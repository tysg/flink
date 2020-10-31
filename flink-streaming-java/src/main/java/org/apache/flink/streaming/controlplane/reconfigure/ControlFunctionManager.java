package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlOperatorFactory;
import org.apache.flink.streaming.controlplane.reconfigure.type.FunctionTypeStorage;
import org.apache.flink.streaming.controlplane.reconfigure.type.InMemoryFunctionStorge;
import org.apache.flink.streaming.controlplane.streammanager.insts.PrimitiveInstruction;

/**
 * Implement Function Transfer
 */
public abstract class ControlFunctionManager implements ControlFunctionManagerService {

	private FunctionTypeStorage functionTypeStorage;
	protected final PrimitiveInstruction primitiveInstruction;

	public ControlFunctionManager(PrimitiveInstruction primitiveInstruction) {
		this.primitiveInstruction = primitiveInstruction;
		this.functionTypeStorage = new InMemoryFunctionStorge();
	}

	public abstract void startControllerInternal();


	/**
	 * we don't know how to register new function yet
	 *
	 * @param function target control function
	 */
	@Override
	public void registerFunction(ControlFunction function) {
		functionTypeStorage.addFunctionType(function.getClass());
	}

}

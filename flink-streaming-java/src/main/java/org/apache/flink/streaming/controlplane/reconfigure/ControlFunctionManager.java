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
public class ControlFunctionManager implements ControlFunctionManagerService {

	private FunctionTypeStorage functionTypeStorage;
	protected final PrimitiveInstruction primitiveInstruction;

	public ControlFunctionManager(PrimitiveInstruction primitiveInstruction) {
		this.primitiveInstruction = primitiveInstruction;
		this.functionTypeStorage = new InMemoryFunctionStorge();
	}

	public void startControllerInternal() {
		System.out.println("Control Function Manager starting...");
	}


	/**
	 * we don't know how to register new function yet
	 *
	 * @param function target control function
	 */
	@Override
	public void registerFunction(ControlFunction function) {
		functionTypeStorage.addFunctionType(function.getClass());
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
			primitiveInstruction.changeOperator(operatorID, operatorFactory);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

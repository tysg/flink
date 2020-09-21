package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.reconfigure.operator.UpdatedOperatorFactory;
import org.apache.flink.streaming.controlplane.reconfigure.type.FunctionTypeStorage;
import org.apache.flink.streaming.controlplane.reconfigure.type.InMemoryFunctionStorge;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerService;

/**
 * Implement Function Transfer
 */
public class ControlFunctionManager implements ControlFunctionManagerService {

	private FunctionTypeStorage functionTypeStorage;
	protected final StreamManagerService streamManagerService;
	protected JobGraphUpdater jobGraphUpdater;

	public ControlFunctionManager(StreamManagerService streamManagerService, JobGraphRescaler jobGraphRescaler) {
		this.streamManagerService = streamManagerService;
		this.functionTypeStorage = new InMemoryFunctionStorge();
		this.jobGraphUpdater = new JobGraphUpdater(jobGraphRescaler, streamManagerService.getJobGraph());
	}

	public void onJobStart() {
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
		UpdatedOperatorFactory operatorFactory = new UpdatedOperatorFactory(
			operatorID,
			streamManagerService.getJobGraph(),
			function);
		try {
			jobGraphUpdater.updateOperator(operatorID, operatorFactory);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Substitute `Control` Function finished!");
	}

}

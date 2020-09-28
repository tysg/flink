package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlOperatorFactory;
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
		this.jobGraphUpdater = new JobGraphUpdater(jobGraphRescaler, streamManagerService);
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
		ControlOperatorFactory<?, ?> operatorFactory = new ControlOperatorFactory<>(
			operatorID,
			streamManagerService.getJobGraph(),
			function);
		try {
			JobVertexID jobVertexID = jobGraphUpdater.updateOperator(operatorID, operatorFactory);
			System.out.println("Substitute `Control` Function finished!");
			// since job graph is shared in stream manager and among its services, we don't need to pass it
			((StreamManagerGateway) streamManagerService).notifyJobGraphOperatorChanged(null, jobVertexID, operatorID);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

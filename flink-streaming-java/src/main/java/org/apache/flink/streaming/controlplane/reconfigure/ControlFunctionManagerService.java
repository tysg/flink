package org.apache.flink.streaming.controlplane.reconfigure;


import org.apache.flink.runtime.jobgraph.OperatorID;

public interface ControlFunctionManagerService {

	void registerFunction(ControlFunction function);

	void reconfigureFunction(OperatorID operatorID, Class<? extends ControlFunction> type);
}

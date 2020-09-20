package org.apache.flink.streaming.controlplane.reconfigure;


import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.reconfigure.operator.UpdatedOperator;

public interface ControlFunctionManagerService {

	void registerFunction(ControlFunction function);

	void reconfigure(OperatorID operatorID, UpdatedOperator operator);
}

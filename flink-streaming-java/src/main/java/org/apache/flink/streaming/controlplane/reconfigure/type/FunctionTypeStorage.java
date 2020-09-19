package org.apache.flink.streaming.controlplane.reconfigure.type;

import org.apache.flink.streaming.controlplane.reconfigure.ControlFunction;

public interface FunctionTypeStorage {

	void addFunctionType(Class<? extends ControlFunction> type);

	ControlFunction getTargetFunction(Class<? extends ControlFunction> type);

}

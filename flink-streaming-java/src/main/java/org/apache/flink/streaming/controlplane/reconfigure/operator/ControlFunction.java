package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.api.common.functions.Function;

public interface ControlFunction extends Function {

	/**
	 *
	 * @param ctx the returning object will be stored in ctx collector
	 * @param input the input data
	 */
	void invokeControl(ControlContext ctx, Object input);

}

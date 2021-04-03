package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

import javax.annotation.Nonnull;
import java.util.Iterator;

public abstract class AbstractControlPolicy implements ControlPolicy {

	private final ReconfigurationAPI reconfigurationAPI;

	protected AbstractControlPolicy(ReconfigurationAPI reconfigurationAPI){
		this.reconfigurationAPI = reconfigurationAPI;
	}

	public ReconfigurationAPI getReconfigurationExecutor() {
		return reconfigurationAPI;
	}

	protected int findOperatorByName(@Nonnull String name) {
		for (Iterator<OperatorDescriptor> it = reconfigurationAPI.getExecutionPlan().getAllOperator(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			if(descriptor.getName().equals(name)){
				return descriptor.getOperatorID();
			}
		}
		return -1;
	}
}

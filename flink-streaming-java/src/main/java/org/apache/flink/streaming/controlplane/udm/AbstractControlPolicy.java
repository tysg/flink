package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

public abstract class AbstractControlPolicy implements ControlPolicy{

	private final ReconfigurationAPI reconfigurationAPI;

	protected AbstractControlPolicy(ReconfigurationAPI reconfigurationAPI){
		this.reconfigurationAPI = reconfigurationAPI;
	}

	public ReconfigurationAPI getInstructionSet() {
		return reconfigurationAPI;
	}
}

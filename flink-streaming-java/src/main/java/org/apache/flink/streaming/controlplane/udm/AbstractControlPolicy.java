package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.streaming.controlplane.streammanager.insts.PrimitiveInstruction;

public abstract class AbstractControlPolicy implements ControlPolicy{

	private final PrimitiveInstruction primitiveInstruction;

	protected AbstractControlPolicy(PrimitiveInstruction primitiveInstruction){
		this.primitiveInstruction = primitiveInstruction;
	}

	public PrimitiveInstruction getInstructionSet() {
		return primitiveInstruction;
	}
}

package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.controlplane.streammanager.insts.OperatorDescriptor;
import org.apache.flink.streaming.controlplane.streammanager.insts.PrimitiveInstruction;
import org.apache.flink.streaming.controlplane.streammanager.insts.StreamJobState;

import java.util.Collections;
import java.util.Iterator;

public class TestingControlPolicy extends AbstractControlPolicy {

	public TestingControlPolicy(PrimitiveInstruction primitiveInstruction) {
		super(primitiveInstruction);
	}

	@Override
	public void startControllers() {
		System.out.println("Testing TestingControlPolicy is starting...");

		new Thread(
			() -> {
				try {
					Thread.sleep(5);

					StreamJobState streamJobState = getInstructionSet().getStreamJobState();
					for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperatorDescriptor(); it.hasNext(); ) {
						OperatorDescriptor descriptor = it.next();
						System.out.println(descriptor);
						System.out.println(streamJobState.getKeyMapping(descriptor.getOperatorID()));
						System.out.println(streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
						System.out.println(streamJobState.getParallelism(descriptor.getOperatorID()));
						System.out.println(streamJobState.getUserFunction(descriptor.getOperatorID()));
					}

					streamJobState.setStateUpdatingFlag(this);
					getInstructionSet().callCustomizeInstruction(
						enforcement -> FutureUtils.completedVoidFuture()
							.thenCompose(o -> enforcement.prepareExecutionPlan())
							.thenCompose(o -> enforcement.synchronizeTasks(Collections.emptyList()))
							.thenCompose(o -> enforcement.updateState())
							.thenAccept(o -> {
								try {
									streamJobState.notifyUpdateFinished(null);
								} catch (Exception e) {
									e.printStackTrace();
								}
							})
					);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		).start();
	}

	@Override
	public void stopControllers() {
		System.out.println("Testing TestingControlPolicy is stopping...");
	}

	@Override
	public void onChangeCompleted(JobVertexID jobVertexID) {
		System.out.println("my self defined instruction finished??");
	}
}

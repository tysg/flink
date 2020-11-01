package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.controlplane.streammanager.insts.PrimitiveInstruction;

import java.util.Collections;

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
					getInstructionSet().getStreamJobState().setStateUpdatingFlag(this);
					getInstructionSet().callCustomizeInstruction(
						enforcement -> {
							FutureUtils.completedVoidFuture()
								.thenCompose(o -> enforcement.prepareExecutionPlan())
								.thenCompose(o -> enforcement.synchronizeTasks(Collections.emptyList()))
								.thenCompose(o -> enforcement.updateState())
								.thenAccept(o -> {
									try {
										getInstructionSet().getStreamJobState().notifyUpdateFinished(null);
									} catch (Exception e) {
										e.printStackTrace();
									}
								});
						}
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

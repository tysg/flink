package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

import java.util.Collections;
import java.util.Iterator;

public class TestingControlPolicy extends AbstractControlPolicy {

	public TestingControlPolicy(ReconfigurationAPI reconfigurationAPI) {
		super(reconfigurationAPI);
	}

	@Override
	public void startControllers() {
		System.out.println("Testing TestingControlPolicy is starting...");

		new Thread(
			() -> {
				try {
					Thread.sleep(5);

					StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
					for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperatorDescriptor(); it.hasNext(); ) {
						OperatorDescriptor descriptor = it.next();
						System.out.println(descriptor);
						System.out.println(streamJobState.getKeyMapping(descriptor.getOperatorID()));
						System.out.println(streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
//						System.out.println(streamJobState.getParallelism(descriptor.getOperatorID()));
//						System.out.println(streamJobState.getUserFunction(descriptor.getOperatorID()));
					}
					// just show how to defined customize operations
					int testingID = findOperatorByName("filte");
					getInstructionSet().callCustomizeOperations(
						enforcement -> FutureUtils.completedVoidFuture()
							.thenCompose(o -> enforcement.synchronizePauseTasks(Collections.singletonList(Tuple2.of(testingID, 1))))
							.thenCompose(o -> enforcement.resumeTasks(Collections.singletonList(Tuple2.of(testingID, 1))))
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
		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
		for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			System.out.println(descriptor);
			try {
				System.out.println(streamJobState.getKeyMapping(descriptor.getOperatorID()));
				System.out.println(streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
			} catch (Exception e) {
				e.printStackTrace();
			}
//						System.out.println(streamJobState.getParallelism(descriptor.getOperatorID()));
//						System.out.println(streamJobState.getUserFunction(descriptor.getOperatorID()));
		}
	}

	@Override
	public void onChangeCompleted(Integer jobVertexID) {
		System.out.println("my self defined instruction finished??");
	}

}

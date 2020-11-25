package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

import java.util.*;
import java.util.stream.Collectors;

public class TestingControlPolicy extends AbstractControlPolicy {

	private final Object object = new Object();

	public TestingControlPolicy(ReconfigurationAPI reconfigurationAPI) {
		super(reconfigurationAPI);
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("Testing TestingControlPolicy is starting...");

		new Thread(
			() -> {
				try {
					StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
					for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperatorDescriptor(); it.hasNext(); ) {
						OperatorDescriptor descriptor = it.next();
						System.out.println(descriptor);
						System.out.println("key mapping:" + streamJobState.getKeyMapping(descriptor.getOperatorID()));
						System.out.println("key state allocation" + streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
						System.out.println("-------------------");
					}
					Thread.sleep(15);
					int testingID = findOperatorByName("Splitter Flatap (but name filter)");
					// just show how to defined customize operations
//					System.out.println("start synchronize test...");
//					getInstructionSet().callCustomizeOperations(
//						enforcement -> FutureUtils.completedVoidFuture()
//							.thenCompose(o -> enforcement.synchronizePauseTasks(Collections.singletonList(Tuple2.of(testingID, 1))))
//							.thenCompose(o -> enforcement.resumeTasks(Collections.singletonList(Tuple2.of(testingID, 1))))
//							.thenAccept(o -> {
//								synchronized (object) {
//									object.notify();
//								}
//							})
//					);
//					synchronized (object) {
//						object.wait();
//					}
					System.out.println("start rebalance test...");
					Map<Integer, List<List<Integer>>> map = streamJobState.getKeyStateAllocation(testingID);
					List<List<Integer>> keySet = map.values().iterator().next();

					List<List<Integer>> newKeySet = keySet.stream()
						.map(ArrayList::new)
						.collect(Collectors.toList());
					List<Integer> oddKeys = newKeySet.get(1).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
					newKeySet.get(0).addAll(oddKeys);
					newKeySet.get(1).removeAll(oddKeys);
					getInstructionSet().rebalance(testingID, newKeySet, this);

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
	public synchronized void onChangeCompleted(Integer jobVertexID) {
		System.out.println("my self defined instruction finished??");
	}

}

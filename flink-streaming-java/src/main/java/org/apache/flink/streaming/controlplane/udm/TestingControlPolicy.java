package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestingControlPolicy extends AbstractControlPolicy {

	private final Object object = new Object();
	private final TestingThread testingThread;

	public TestingControlPolicy(ReconfigurationAPI reconfigurationAPI) {
		super(reconfigurationAPI);
		testingThread = new TestingThread();
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("Testing TestingControlPolicy is starting...");
		testingThread.setName("reconfiguration test");
		testingThread.start();
	}

	@Override
	public void stopControllers() {
		System.out.println("Testing TestingControlPolicy is stopping...");
		showOperatorInfo();
	}

	@Override
	public synchronized void onChangeCompleted(Integer jobVertexID) {
		System.out.println("my self defined instruction finished??");
		synchronized (object) {
			object.notify();
		}
	}

	private void showOperatorInfo() {
		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
		for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperatorDescriptor(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			System.out.println(descriptor);
			System.out.println("key mapping:" + streamJobState.getKeyMapping(descriptor.getOperatorID()));
			System.out.println("key state allocation" + streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
			System.out.println("-------------------");
		}
	}

	private void testCustomizeAPI(int testingOpID) throws InterruptedException {
		//	 just show how to defined customize operations
		System.out.println("start synchronize test...");
		getInstructionSet().callCustomizeOperations(
			enforcement -> FutureUtils.completedVoidFuture()
				.thenCompose(o -> enforcement.synchronizePauseTasks(Collections.singletonList(Tuple2.of(testingOpID, 1))))
				.thenCompose(o -> enforcement.resumeTasks(Collections.singletonList(Tuple2.of(testingOpID, 1))))
				.thenAccept(o -> {
					synchronized (object) {
						object.notify();
					}
				})
		);
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	private void testRebalanceStateful(int testingOpID) throws InterruptedException {
		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
		List<List<Integer>> keySet = streamJobState.getKeyStateAllocation(testingOpID);
		// we assume that each operator only have one input now

		List<List<Integer>> newKeySet = keySet.stream()
			.map(ArrayList::new)
			.collect(Collectors.toList());
		List<Integer> oddKeys = newKeySet.get(0).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
		newKeySet.get(0).removeAll(oddKeys);
		newKeySet.get(1).addAll(oddKeys);
		getInstructionSet().rebalance(testingOpID, newKeySet, true, this);
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	private void testRebalanceStateless(int testingOpID) throws InterruptedException {
		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
		Set<OperatorDescriptor> parents = streamJobState.getOperatorDescriptorByID(testingOpID).getParents();
		// we assume that each operator only have one input now
		for (OperatorDescriptor parent : parents) {
			List<List<Integer>> newKeySet = parent.getKeyMapping()
				.get(testingOpID)
				.stream()
				.map(ArrayList::new)
				.collect(Collectors.toList());
			List<Integer> oddKeys = newKeySet.get(0).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
			newKeySet.get(0).removeAll(oddKeys);
			newKeySet.get(1).addAll(oddKeys);
			getInstructionSet().rebalance(testingOpID, newKeySet, false, this);
			// wait for operation completed
			synchronized (object) {
				object.wait();
			}
			break;
		}
	}

	private void testPauseSource(int sourceID) throws InterruptedException {
		getInstructionSet().callCustomizeOperations(
			enforcement -> FutureUtils.completedVoidFuture()
				.thenCompose(o -> enforcement.synchronizePauseTasks(Collections.singletonList(Tuple2.of(sourceID, -1))))
				.thenCompose(o -> {
					try {
						Thread.sleep(3);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return enforcement.resumeTasks(Collections.singletonList(Tuple2.of(sourceID, -1)));
				})
				.thenAccept(o -> {
					synchronized (object) {
						object.notify();
					}
				})
		);
		// wait for operation completed
		synchronized (object) {
			object.wait();
			System.out.println("pause source successful");
		}
	}

	private class TestingThread extends Thread {

		@Override
		public void run() {
			// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
			int statefulOpID = findOperatorByName("Splitter Flatmap");
			int statelessOpID = findOperatorByName("filter");
			int sourceOp = findOperatorByName("Source: source");
			// this operator is the downstream of actual source operator
			int nearSourceMap = findOperatorByName("near source Flatmap");
			int nearSourceFilter = findOperatorByName("source filter");
			if (statefulOpID == -1 || statelessOpID == -1
				|| nearSourceMap == -1 || nearSourceFilter == -1
				|| sourceOp == -1) {
				System.out.println("can not find operator with given name, corrupt");
				return;
			}
			try {
				showOperatorInfo();
				Thread.sleep(10);
				System.out.println("\nstart testCustomizeAPI test...");
				testCustomizeAPI(statefulOpID);

				System.out.println("\nstart stateless rebalance test...");
				testRebalanceStateless(statelessOpID);

				System.out.println("\nstart stateful rebalance test1...");
				testRebalanceStateful(statefulOpID);

				System.out.println("\nstart stateful rebalance test2...");
				testRebalanceStateful(statefulOpID);

				System.out.println("\nstart synchronize source test...");
				testPauseSource(sourceOp);

				System.out.println("\nstart source near stateful operator rebalance test...");
				testRebalanceStateful(nearSourceMap);

				System.out.println("\nstart source near stateless operator rebalance test...");
				testRebalanceStateless(nearSourceFilter);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

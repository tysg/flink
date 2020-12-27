package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

import java.util.*;
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

	private void testScaleOutStateful(int testingOpID) throws InterruptedException {
		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		List<List<Integer>> keySet = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == keySet.size() : "old parallelism does not match the key set";

		List<List<Integer>> newKeySet = keySet.stream()
				.map(ArrayList::new)
				.collect(Collectors.toList());

		List<Integer> smallHalf = newKeySet.get(oldParallelism-1).stream()
			.filter(i -> i < 63) // hardcoded
			.collect(Collectors.toList());
		newKeySet.get(oldParallelism-1).removeAll(smallHalf);
		newKeySet.add(smallHalf);

		System.out.println(newKeySet);

		getInstructionSet().rescale(testingOpID, oldParallelism+1, newKeySet, this);

		synchronized (object) {
			object.wait();
		}
	}

	private void testScaleOut2(int testingOpID) throws InterruptedException {
		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		List<List<Integer>> keySet = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == keySet.size() : "old parallelism does not match the key set";

		List<List<Integer>> newKeySet = keySet.stream()
			.map(ArrayList::new)
			.collect(Collectors.toList());

		List<Integer> smallHalf1 = newKeySet.get(oldParallelism-1).stream()
			.filter(i -> i < 31) // hardcoded
			.collect(Collectors.toList());
		newKeySet.get(oldParallelism-1).removeAll(smallHalf1);
		newKeySet.add(smallHalf1);
		List<Integer> smallHalf2 = newKeySet.get(oldParallelism-1).stream()
			.filter(i -> i < 63) // hardcoded
			.collect(Collectors.toList());
		newKeySet.get(oldParallelism-1).removeAll(smallHalf2);
		newKeySet.add(smallHalf2);

		System.out.println(newKeySet);

		getInstructionSet().rescale(testingOpID, oldParallelism+2, newKeySet, this);

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
				.thenCompose(o -> enforcement.synchronizePauseTasks(Collections.singletonList(Tuple2.of(sourceID, -1)), null))
				.thenCompose(o -> {
					try {
						Thread.sleep(3);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return enforcement.resumeTasks();
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

	private void testCustomizeWindowUpdateAPI() throws InterruptedException {
		//	 an example shows how to defined customize operations
		int windowOpID = findOperatorByName("counting window reduce");
		if (windowOpID != -1) {
			OperatorDescriptor descriptor = getInstructionSet().getJobExecutionPlan().getOperatorDescriptorByID(windowOpID);
			Map<String, Object> attributeMap = descriptor.getControlAttributeMap();
			PurgingTrigger<?, ?> trigger = (PurgingTrigger<?, ?>) attributeMap.get("trigger");
			long oldWindowSize = ((CountTrigger<?>) trigger.getNestedTrigger()).getMaxCount();
			System.out.println("update window size from " + oldWindowSize + " to " + (oldWindowSize / 2));
			try {
				updateCountingWindowSize(windowOpID, oldWindowSize / 2);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// self customize high level reconfiguration api
	private void updateCountingWindowSize(int rawVertexID, long newWindowSize) throws Exception {
		// update abstraction in stream manager execution plan
		CountTrigger<?> trigger = CountTrigger.of(newWindowSize);
		OperatorDescriptor descriptor = getInstructionSet().getJobExecutionPlan().getOperatorDescriptorByID(rawVertexID);
		descriptor.setControlAttribute("trigger", PurgingTrigger.of(trigger));
		getInstructionSet().callCustomizeOperations(
			enforcement -> FutureUtils.completedVoidFuture()
				.thenCompose(o -> enforcement.prepareExecutionPlan(getInstructionSet().getJobExecutionPlan()))
				.thenCompose(o -> enforcement.synchronizePauseTasks(Collections.singletonList(Tuple2.of(rawVertexID, -1)), o))
				.thenCompose(o -> enforcement.updateFunction(rawVertexID, o))
				.thenCompose(o -> enforcement.resumeTasks())
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

	private void testScaleOutWindowJoin() throws InterruptedException {

		StreamJobExecutionPlan streamJobState = getInstructionSet().getJobExecutionPlan();
		int testingOpID = findOperatorByName("join1");

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		List<List<Integer>> keySet = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == keySet.size() : "old parallelism does not match the key set";

		List<List<Integer>> newKeySet = keySet.stream()
			.map(ArrayList::new)
			.collect(Collectors.toList());

		List<Integer> oddKeys = newKeySet.get(0).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
		newKeySet.get(0).removeAll(oddKeys);
		newKeySet.add(oddKeys);

		System.out.println(newKeySet);

		getInstructionSet().rescale(testingOpID, oldParallelism+1, newKeySet, this);

		synchronized (object) {
			object.wait();
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
			int nearSourceFilter = findOperatorByName("source stateless map");
			if (statefulOpID == -1 || statelessOpID == -1
				|| nearSourceMap == -1 || nearSourceFilter == -1
				|| sourceOp == -1) {
				System.out.println("can not find operator with given name, corrupt");
				return;
			}
			try {
				showOperatorInfo();
				Thread.sleep(10);

				System.out.println("\nstart stateless rebalance test...");
				testRebalanceStateless(statelessOpID);

				System.out.println("\nstart stateful rebalance test1...");
				testRebalanceStateful(statefulOpID);

				System.out.println("\nstart stateful rebalance test2...");
				testRebalanceStateful(statefulOpID);

//				System.out.println("\nst.art synchronize source test...");
//				testPauseSource(sourceOp);

				System.out.println("\nstart stateful scale out test");
				testScaleOutStateful(statefulOpID);

				System.out.println("\nstart source near stateful operator rebalance test...");
				testRebalanceStateful(nearSourceMap);

				System.out.println("\nstart source near stateless operator rebalance test...");
				testRebalanceStateless(nearSourceFilter);

				Thread.sleep(3000);

				System.out.println("\nstart update function related test...");
				testCustomizeWindowUpdateAPI();

				System.out.println("\nstart rescale window join test...");
				testScaleOutWindowJoin();

				System.out.println("\nstart stateful scale out 2 more test");
				testScaleOut2(statelessOpID);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

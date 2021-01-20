package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

import java.util.*;

public class PerformanceEvaluator extends AbstractControlPolicy {

	private final Object object = new Object();
	private final TestingThread testingThread;
	private final Map<String, String> experimentConfig;

	public final static String AFFECTED_TASK = "test.affectedTask";
	public final static String TEST_OPERATOR_NAME = "testOperator.name";
	public final static String MAX_CONFIG_PARALLELISM = "testOperator.maxParallelism";
	public final static String RECONFIG_FREQUENCY = "reconfiguration.frequency";
	public final static String TEST_TYPE = "test.type";

	private final static String REMAP = "remap";
	private final static String SCALE = "scale";
	private final static String NOOP = "noop";
	private final static String EXECUTION_LOGIC = "logic";

	public PerformanceEvaluator(ReconfigurationAPI reconfigurationAPI, Configuration configuration) {
		super(reconfigurationAPI);
		testingThread = new TestingThread();
		experimentConfig = configuration.toMap();
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("PerformanceMeasure is starting...");
		testingThread.setName("reconfiguration performance measure");
		testingThread.start();
	}

	@Override
	public void stopControllers() {
		System.out.println("PerformanceMeasure is stopping...");
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if(throwable != null){
			testingThread.interrupt();
			return;
		}
		synchronized (object) {
			object.notify();
		}
	}

	protected void generateTest() throws InterruptedException {
		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		int numAffectedTasks = Integer.parseInt(experimentConfig.getOrDefault(AFFECTED_TASK, "3"));
		int reconfigFreq = Integer.parseInt(experimentConfig.getOrDefault(RECONFIG_FREQUENCY, "5"));
		int testOpID = findOperatorByName(testOperatorName);
		switch (experimentConfig.getOrDefault(TEST_TYPE, SCALE)) {
			case REMAP:
				measureRebalance(testOpID, numAffectedTasks, reconfigFreq);
				break;
			case SCALE:
				int newParallelism = Integer.parseInt(experimentConfig.getOrDefault(MAX_CONFIG_PARALLELISM, "10"));
				measureRescale(testOpID, numAffectedTasks, newParallelism, reconfigFreq);
				break;
			case NOOP:
				measureNoOP(testOpID, reconfigFreq);
				break;
		}
	}

	private void measureRebalance(int testOpID, int numAffectedTasks, int reconfigFreq) throws InterruptedException {
		StreamJobExecutionPlan executionPlan = getInstructionSet().getJobExecutionPlan();
		if (reconfigFreq > 0) {
			int timeInterval = 1000 / reconfigFreq;
			int i = 0;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				long start = System.currentTimeMillis();
				Map<Integer, List<Integer>> keySet = executionPlan.getKeyStateAllocation(testOpID);
				Map<Integer, List<Integer>> newKeySet = new HashMap<>();
				for (Integer taskId : keySet.keySet()) {
					newKeySet.put(taskId, new ArrayList<>(keySet.get(taskId)));
				}
				// random select numAffectedTask sub key set, and then shuffle them to the same number of key set
				shuffleKeySet(newKeySet, numAffectedTasks);
				System.out.println("\nnumber of rebalance test: " + i);
				System.out.println("new key set:" + newKeySet);
				getInstructionSet().rebalance(testOpID, newKeySet, true, this);
				// wait for operation completed
				synchronized (object) {
					object.wait();
				}
				while ((System.currentTimeMillis() - start) < timeInterval) {
				}
				i++;
			}
		}
	}

	private void measureRescale(int testOpID, int numAffectedTasks, int maxParallelism, int reconfigFreq) throws InterruptedException {
		StreamJobExecutionPlan executionPlan = getInstructionSet().getJobExecutionPlan();
		if (reconfigFreq > 0) {
			int timeInterval = 1000 / reconfigFreq;
			int i = 0;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				long start = System.currentTimeMillis();
				Map<Integer, List<Integer>> keySet = executionPlan.getKeyStateAllocation(testOpID);
				Map<Integer, List<Integer>> newKeySet = new HashMap<>();
				for (Integer taskId : keySet.keySet()) {
					newKeySet.put(taskId, new ArrayList<>(keySet.get(taskId)));
				}
				Random random = new Random();
				int current = executionPlan.getParallelism(testOpID);
				int newParallelism = 1 + random.nextInt(maxParallelism);
				if (newParallelism > current) {
					shuffleKeySetWhenScaleOut(newKeySet, newParallelism, numAffectedTasks);
				} else if (newParallelism < current) {
//					continue;
					shuffleKeySetWhenScaleIn(newKeySet, newParallelism, numAffectedTasks);
				} else {
					continue;
				}
				// random select numAffectedTask sub key set, and then shuffle them to the same number of key set
				shuffleKeySet(newKeySet, numAffectedTasks);
				System.out.println("\nnumber of rescale test: " + i);
				System.out.println("new key set:" + newKeySet);
				getInstructionSet().rescale(testOpID, newParallelism, newKeySet, this);
				// wait for operation completed
				synchronized (object) {
					object.wait();
				}
				while ((System.currentTimeMillis() - start) < timeInterval) {
					// busy waiting
				}
				i++;
			}
		}
	}

	private void measureNoOP(int testOpID, int reconfigFreq) throws InterruptedException {
		StreamJobExecutionPlan executionPlan = getInstructionSet().getJobExecutionPlan();
		if (reconfigFreq > 0) {
			int timeInterval = 1000 / reconfigFreq;
			int i = 0;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				long start = System.currentTimeMillis();
				System.out.println("\nnumber of noop test: " + i);
				getInstructionSet().noOp(testOpID, this);
				// wait for operation completed
				synchronized (object) {
					object.wait();
				}
				if (System.currentTimeMillis() - start > timeInterval) {
					System.out.println("overloaded frequency");
				}
				while ((System.currentTimeMillis() - start) < timeInterval) {
				}
				i++;
			}
		}
	}

	private void shuffleKeySet(Map<Integer, List<Integer>> newKeySet, int numAffectedTasks) {
		Random random = new Random();
		List<Integer> selectTaskID = new ArrayList<>(numAffectedTasks);
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		for (int i = 0; i < numAffectedTasks; i++) {
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
		}
		List<Integer> keyGroupList = new LinkedList<>(allKeyGroup);
		int leftBound = 0;
		for (int i = 0; i < numAffectedTasks - 1; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
			int subKeySetSize = random.nextInt(keyGroupList.size() - leftBound - numAffectedTasks + i + 2);
			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
			newKeySet.put(
				selectTaskID.get(i),
				new ArrayList<>(keyGroupList.subList(leftBound, leftBound + subKeySetSize))
			);
			leftBound += subKeySetSize;
		}
		newKeySet.put(
			selectTaskID.get(numAffectedTasks - 1),
			new ArrayList<>(keyGroupList.subList(leftBound, keyGroupList.size()))
		);
	}

	private List<Integer> findNextSubTaskID(Collection<Integer> keySetID, int numOfNext) {
		List<Integer> next = new LinkedList<>();
		for (int i = 0; i < numOfNext; i++) {
			int id = 0;
			while (keySetID.contains(id) || next.contains(id)) {
				id++;
			}
			next.add(id);
		}
		return next;
	}

	private void shuffleKeySetWhenScaleOut(Map<Integer, List<Integer>> newKeySet, int newParallelism, int numAffectedTasks) {
		Random random = new Random();
		// add new added key set id
		List<Integer> selectTaskID = new ArrayList<>(findNextSubTaskID(newKeySet.keySet(), newParallelism - newKeySet.size()));
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		// add affected task key set id
		for (int i = 0; i < numAffectedTasks; i++) {
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
		}

		List<Integer> keyGroupList = new LinkedList<>(allKeyGroup);
		int leftBound = 0;
		for (int i = 0; i < selectTaskID.size() - 1; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
			int subKeySetSize = random.nextInt(keyGroupList.size() - leftBound - selectTaskID.size() + i + 2);
			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
			newKeySet.put(
				selectTaskID.get(i),
				new ArrayList<>(keyGroupList.subList(leftBound, leftBound + subKeySetSize))
			);
			leftBound += subKeySetSize;
		}
		newKeySet.put(
			selectTaskID.get(selectTaskID.size() - 1),
			new ArrayList<>(keyGroupList.subList(leftBound, keyGroupList.size()))
		);
	}

	private void shuffleKeySetWhenScaleIn(Map<Integer, List<Integer>> newKeySet, int newParallelism, int numAffectedTasks) {
		Random random = new Random();
		int numOfRemove = newKeySet.size() - newParallelism;
		numAffectedTasks = numAffectedTasks > numOfRemove ? numAffectedTasks : numOfRemove + 1;
		int keeped = numAffectedTasks - numOfRemove;
		List<Integer> selectTaskID = new ArrayList<>(numAffectedTasks);
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		for (int i = 0; i < numAffectedTasks; i++) {
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
		}
		List<Integer> keyGroupList = new LinkedList<>(allKeyGroup);
		int leftBound = 0;
		Collections.sort(selectTaskID);
		for (int i = 0; i < keeped - 1; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
			int subKeySetSize = random.nextInt(keyGroupList.size() - leftBound - keeped + i + 2);
			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
			newKeySet.put(
				selectTaskID.get(i),
				new ArrayList<>(keyGroupList.subList(leftBound, leftBound + subKeySetSize))
			);
			leftBound += subKeySetSize;
		}
		newKeySet.put(
			selectTaskID.get(keeped - 1),
			new ArrayList<>(keyGroupList.subList(leftBound, keyGroupList.size()))
		);
	}

	private void testNoOp(int operatorID) throws InterruptedException {

		getInstructionSet().noOp(operatorID, this);

		synchronized (object) {
			object.wait();
		}
	}

	private class TestingThread extends Thread {

		@Override
		public void run() {
			// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
			try {
				Thread.sleep(5000);
				generateTest();
				Thread.sleep(3000);
//				File latencyFile = new File("/home/hya/prog/latency.out");
//				File copy = new File("/home/hya/prog/latency.out.copy");
//				Files.copy(latencyFile.toPath(), copy.toPath());
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.err.println("interrupted, thread exit");
			}
		}
	}

}

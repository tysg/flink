package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

public class PerformanceEvaluator extends AbstractControlPolicy {
	private static final Logger LOG = LoggerFactory.getLogger(PerformanceEvaluator.class);

	private final Object object = new Object();
	private final TestingThread testingThread;
	private final Map<String, String> experimentConfig;

	public final static String AFFECTED_TASK = "trisk.reconfig.affected_tasks";
	public final static String TEST_OPERATOR_NAME = "trisk.reconfig.operator.name";
	public final static String RECONFIG_FREQUENCY = "trisk.reconfig.frequency";
	public final static String TEST_TYPE = "trisk.reconfig.type";

	private final static String REMAP = "remap";
	private final static String RESCALE = "rescale";
	private final static String NOOP = "noop";
	private final static String EXECUTION_LOGIC = "logic";

	private boolean finished = false;

	private int latestUnusedSubTaskIdx = 0;

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
		finished = true;
		testingThread.interrupt();
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if (throwable != null) {
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
		latestUnusedSubTaskIdx = getInstructionSet().getJobExecutionPlan().getParallelism(testOpID);
		switch (experimentConfig.getOrDefault(TEST_TYPE, RESCALE)) {
			case REMAP:
				measureRebalance(testOpID, numAffectedTasks, reconfigFreq);
				break;
			case RESCALE:
				measureRescale(testOpID, numAffectedTasks, 10, reconfigFreq);
				break;
			case EXECUTION_LOGIC:
				measureFunctionUpdate(testOpID, reconfigFreq);
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
			long start;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				start = System.currentTimeMillis();
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
				while ((System.currentTimeMillis() - start) < timeInterval) {}
				i++;
			}
		}
	}

	private void measureFunctionUpdate(int testOpID, int reconfigFreq) throws InterruptedException {
		StreamJobExecutionPlan executionPlan = getInstructionSet().getJobExecutionPlan();
		try {
			ClassLoader userClassLoader = executionPlan.getUserFunction(testOpID).getClass().getClassLoader();
			Class IncreaseCommunicationOverheadMapClass = userClassLoader.loadClass("flinkapp.StatefulDemoLongRun$IncreaseCommunicationOverheadMap");
			Class IncreaseComputationOverheadMap = userClassLoader.loadClass("flinkapp.StatefulDemoLongRun$IncreaseComputationOverheadMap");
			if (reconfigFreq > 0) {
				int timeInterval = 1000 / reconfigFreq;
				int i = 0;
				Random random = new Random();
				while (true) {
					long start = System.currentTimeMillis();
					Object func = null;
					if (random.nextInt(2) > 0) {
						func = IncreaseCommunicationOverheadMapClass.getConstructor(int.class)
							.newInstance(random.nextInt(10) + 1);
					} else {
						func = IncreaseComputationOverheadMap.getConstructor(int.class)
							.newInstance(random.nextInt(10));
					}
					System.out.println("\nnumber of function update test: " + i);
					System.out.println("new function:" + func);
					getInstructionSet().reconfigureUserFunction(testOpID, func, this);
					// wait for operation completed
					synchronized (object) {
						object.wait();
					}
					while ((System.currentTimeMillis() - start) < timeInterval) {
					}
					i++;
				}
			}
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	private void measureRescale(int testOpID, int numAffectedTasks, int maxParallelism, int reconfigFreq) throws InterruptedException {
		StreamJobExecutionPlan executionPlan = getInstructionSet().getJobExecutionPlan();
		if (reconfigFreq > 0) {
			int timeInterval = 1000 / reconfigFreq;
			int i = 0;
			long start;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				start = System.currentTimeMillis();
				Map<Integer, List<Integer>> keySet = executionPlan.getKeyStateAllocation(testOpID);
				Map<Integer, List<Integer>> newKeySet = new HashMap<>();
				for (Integer taskId : keySet.keySet()) {
					newKeySet.put(taskId, new ArrayList<>(keySet.get(taskId)));
				}
				Random random = new Random();
				int current = executionPlan.getParallelism(testOpID);
				int newParallelism = 1 + random.nextInt(maxParallelism);
				boolean isScaleOut = true;
				if (newParallelism > current) {
					shuffleKeySetWhenScaleOut(newKeySet, newParallelism, numAffectedTasks);
				} else if (newParallelism < current) {
//					continue;
					shuffleKeySetWhenScaleIn(newKeySet, newParallelism, numAffectedTasks);
					isScaleOut = false;
				} else {
					continue;
				}
				// random select numAffectedTask sub key set, and then shuffle them to the same number of key set
				System.out.println("\nnumber of rescale test: " + i);
				System.out.println("new key set:" + newKeySet);

				LOG.info("++++++number of rescale test: " + i + " type: " + (isScaleOut? "scale_out" : "scale_in"));
				LOG.info("++++++new key set: " + newKeySet);

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
			long start;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				start = System.currentTimeMillis();
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
		numAffectedTasks = Math.min(numAffectedTasks, newKeySet.size());
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
			int subKeySetSize = random.nextInt(keyGroupList.size() - leftBound - numAffectedTasks + i + 1) + 1;
//			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
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
		int newParallelism = latestUnusedSubTaskIdx + numOfNext;
		while(latestUnusedSubTaskIdx < newParallelism) {
			checkState(!keySetID.contains(latestUnusedSubTaskIdx) && !next.contains(latestUnusedSubTaskIdx),
				"subtask index has already been used.");
			next.add(latestUnusedSubTaskIdx);
			latestUnusedSubTaskIdx++;
		}
		return next;
	}

	private void shuffleKeySetWhenScaleOut(Map<Integer, List<Integer>> newKeySet, int newParallelism, int numAffectedTasks) {
		Random random = new Random();
		// add new added key set id
		List<Integer> selectTaskID = new ArrayList<>(findNextSubTaskID(newKeySet.keySet(), newParallelism - newKeySet.size()));
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		numAffectedTasks = Math.min(numAffectedTasks, newKeySet.size());
		// add affected task key set id
//		for (int i = 0; i < numAffectedTasks; i++) {
//			int offset = random.nextInt(newKeySet.size());
//			selectTaskID.add(allTaskID.get(offset));
//			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
//		}
		int numOfAddedSubset = 0;
		while (numOfAddedSubset < numAffectedTasks || allKeyGroup.size() < selectTaskID.size()) {
			// allKeyGroup.size() < selectTaskID.size() means we don't have enough key groups
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
			numOfAddedSubset++;
		}

		List<Integer> keyGroupList = new LinkedList<>(allKeyGroup);
		int leftBound = 0;
		for (int i = 0; i < selectTaskID.size() - 1; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
			int subKeySetSize = 1 + random.nextInt(keyGroupList.size() - leftBound - selectTaskID.size() + i + 1);
//			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
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
		numAffectedTasks = Math.min(numAffectedTasks, newKeySet.size());
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
			int subKeySetSize = 1 + random.nextInt(keyGroupList.size() - leftBound - keeped + i + 1);
//			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
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
				if (finished) {
					System.err.println("PerformanceEvaluator stopped");
					return;
				}
				e.printStackTrace();
				System.err.println("interrupted, thread exit");
			}
		}
	}

}

package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.StreamJobExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationAPI;

import java.util.*;

public class PerformanceMeasure extends AbstractControlPolicy {

	private final Object object = new Object();
	private final TestingThread testingThread;
	private final Map<String, String> experimentConfig;

	public final static String AFFECTED_TASK = "test.affectedTask";
	public final static String TEST_OPERATOR_NAME = "testOperator.name";
	public final static String RECONFIG_FREQUENCY = "reconfiguration.frequency";
	public final static String TEST_TYPE = "test.type";

	private final static String REMAP = "remap";
	private final static String SCALE = "scale";

	public PerformanceMeasure(ReconfigurationAPI reconfigurationAPI, Configuration configuration) {
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
	public synchronized void onChangeCompleted(Integer jobVertexID) {
		synchronized (object) {
			object.notify();
		}
	}

	protected void generateTest() throws InterruptedException {
		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		int numAffectedTasks = Integer.parseInt(experimentConfig.getOrDefault(AFFECTED_TASK, "3"));
		int reconfigFreq = Integer.parseInt(experimentConfig.getOrDefault(RECONFIG_FREQUENCY, "5"));
		switch (experimentConfig.getOrDefault(TEST_TYPE, SCALE)) {
			case REMAP:
				int testOpID = findOperatorByName(testOperatorName);
				measureRebalance(testOpID, numAffectedTasks, reconfigFreq);
				break;
			case SCALE:
		}
	}

	private void measureRebalance(int testOpID, int numAffectedTasks, int reconfigFreq) throws InterruptedException {
		StreamJobExecutionPlan executionPlan = getInstructionSet().getJobExecutionPlan();
		for (int i = 0; i < reconfigFreq; i++) {
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
			int subKeySetSize = random.nextInt(keyGroupList.size() - leftBound - numAffectedTasks + i - 1) + 1;
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
				generateTest();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

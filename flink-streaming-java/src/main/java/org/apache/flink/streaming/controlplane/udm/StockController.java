package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

public class StockController extends AbstractController {
	private static final Logger LOG = LoggerFactory.getLogger(StockController.class);

	private final Object lock = new Object();
	private final Profiler profiler;
	private final Map<String, String> experimentConfig;

	public final static String AFFECTED_TASK = "trisk.reconfig.affected_tasks";
	public final static String TEST_OPERATOR_NAME = "trisk.reconfig.operator.name";
	public final static String RECONFIG_FREQUENCY = "trisk.reconfig.frequency";
	public final static String RECONFIG_INTERVAL = "trisk.reconfig.interval";
	public final static String TEST_TYPE = "trisk.reconfig.type";

	private final static String REMAP = "remap";
	private final static String RESCALE = "rescale";
	private final static String NOOP = "noop";
	private final static String EXECUTION_LOGIC = "logic";

	private boolean finished = false;

	private int latestUnusedSubTaskIdx = 0;

	public StockController(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor);
		profiler = new Profiler();
		experimentConfig = configuration.toMap();
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("PerformanceMeasure is starting...");
		profiler.setName("reconfiguration performance measure");
		profiler.start();
	}

	@Override
	public void stopControllers() {
		System.out.println("PerformanceMeasure is stopping...");
		finished = true;
		profiler.interrupt();
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if (throwable != null) {
			profiler.interrupt();
			return;
		}
		synchronized (lock) {
			lock.notify();
		}
	}

	protected void generateTest() throws InterruptedException {
		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		//		int reconfigFreq = Integer.parseInt(experimentConfig.getOrDefault(RECONFIG_FREQUENCY, "5"));
		int testOpID = findOperatorByName(testOperatorName);
		latestUnusedSubTaskIdx = getReconfigurationExecutor().getExecutionPlan().getParallelism(testOpID);
		long start = System.currentTimeMillis();
		boolean rebalanced = false;
		boolean rescaled1 = false;
		boolean rescaled2 = false;
		boolean rescaled3 = false;
		boolean rescaled4 = false;
		// 5s
		Thread.sleep(5000);
		loadBalancingAll(testOpID);
		// 100s
		Thread.sleep(95000);
		scaleOutOne(testOpID);
		// 200s
		Thread.sleep(100000);
		scaleOutOne(testOpID);
		// 400s
		Thread.sleep(200000);
		scaleInOne(testOpID);

//		while (true) {
//			// load balancing
//			if (System.currentTimeMillis() - start > 5000 && !rebalanced) {
//				loadBalancingAll(testOpID);
//				rebalanced = true;
//			}
//			// scale out
//			if (System.currentTimeMillis() - start > 100000 && !rescaled1) {
//				scaleOutOne(testOpID);
//				rescaled1 = true;
//			}
////			// scale out
//			if (System.currentTimeMillis() - start > 200000 && !rescaled2) {
//				scaleOutOne(testOpID);
//				rescaled2 = true;
//			}
//			// scale in
//			if (System.currentTimeMillis() - start > 400000 && !rescaled3) {
//				scaleInOne(testOpID);
//				rescaled3 = true;
//			}
////			// scale in
////			if (System.currentTimeMillis() - start > 400000 && !rescaled4) {
////				scaleInOne(testOpID);
////				rescaled4 = true;
////			}
//		}
	}

	private void waitForCompletion() throws InterruptedException {
		// wait for operation completed
		synchronized (lock) {
			lock.wait();
		}
	}

	private void loadBalancingAll(int testingOpID) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getExecutionPlan();
		scaling(testingOpID, executionPlan.getParallelism(testingOpID));
	}

	private void scaleOutOne(int testingOpID) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getExecutionPlan();
		scaling(testingOpID, executionPlan.getParallelism(testingOpID) + 1);
	}

	private void scaleInOne(int testingOpID) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getExecutionPlan();
		scaling(testingOpID, executionPlan.getParallelism(testingOpID) - 1);
	}

	private void scaling(int testingOpID, int newParallelism) throws InterruptedException {
		System.out.println("++++++ start scaling");
		ExecutionPlan executionPlan = getReconfigurationExecutor().getExecutionPlan();
		OperatorDescriptor targetDescriptor = executionPlan.getOperatorByID(testingOpID);


		Map<Integer, List<Integer>> curKeyStateAllocation = targetDescriptor.getKeyStateAllocation();
		int oldParallelism = targetDescriptor.getParallelism();
		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = preparePartitionAssignment(newParallelism);

		int maxParallelism = 128;

		for (int i = 0; i < maxParallelism; i++) {
			newKeyStateAllocation.get(i%newParallelism).add(i);
		}

		System.out.println(newKeyStateAllocation);

		// update the parallelism
		targetDescriptor.setParallelism(newParallelism);
		boolean isScaleIn = oldParallelism > newParallelism;

		// update the key set
		for (OperatorDescriptor parent : targetDescriptor.getParents()) {
			parent.updateKeyMapping(testingOpID, newKeyStateAllocation);
		}

		System.out.println(newKeyStateAllocation);

		if (oldParallelism == newParallelism) {
//			getReconfigurationExecutor().rebalance(testingOpID, newKeyStateAllocation, true, this);
			getReconfigurationExecutor().rebalance(executionPlan, testingOpID, this);
		} else {
//			getReconfigurationExecutor().rescale(testingOpID, newParallelism, newKeyStateAllocation, this);
			getReconfigurationExecutor().rescale(executionPlan, testingOpID, isScaleIn, this);
		}

		waitForCompletion();
	}

	private Map<Integer, List<Integer>> preparePartitionAssignment(int parallleism) {
		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (int i = 0; i < parallleism; i++) {
			newKeyStateAllocation.put(i, new ArrayList<>());
		}
		return newKeyStateAllocation;
	}

	private class Profiler extends Thread {

		@Override
		public void run() {
			// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
			try {
				Thread.sleep(5000);
				generateTest();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.streaming.controlplane.streammanager.insts.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationExecutor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbstractController implements ControlPolicy {

	private final ReconfigurationExecutor reconfigurationExecutor;
	private final Object lock = new Object();

	protected ControlActionRunner controlActionRunner = new ControlActionRunner();

	@Override
	public void onChangeStarted() throws InterruptedException {
		// wait for operation completed
		synchronized (lock) {
			lock.wait();
		}
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if(throwable != null){
			throw new RuntimeException("error while execute reconfiguration", throwable);
		}
		System.out.println("my self defined instruction finished??");
		synchronized (lock) {
			lock.notify();
		}
	}

	protected AbstractController(ReconfigurationExecutor reconfigurationExecutor){
		this.reconfigurationExecutor = reconfigurationExecutor;
	}

	public ReconfigurationExecutor getReconfigurationExecutor() {
		return reconfigurationExecutor;
	}

	protected int findOperatorByName(@Nonnull String name) {
		for (Iterator<OperatorDescriptor> it = reconfigurationExecutor.getExecutionPlan().getAllOperator(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			if(descriptor.getName().equals(name)){
				return descriptor.getOperatorID();
			}
		}
		return -1;
	}

	protected void remap(int operatorId, Map<Integer, List<Integer>> newKeyDistribution) throws InterruptedException {
		ExecutionPlanWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.redistribute(operatorId, newKeyDistribution);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected void rescale(int operatorId, Map<Integer, List<Integer>> newKeyDistribution,
						   @Nullable Map<Integer, ExecutionPlan.Node> deployment, Boolean isCreate) throws InterruptedException {
		ExecutionPlanWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.redistribute(operatorId, newKeyDistribution)
			.redeploy(operatorId, deployment, isCreate);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected void replacement(Integer operatorId, @Nullable Map<Integer, ExecutionPlan.Node> deployment, Boolean isCreate) throws InterruptedException {
		ExecutionPlanWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.redeploy(operatorId, deployment, isCreate);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected void changeOfLogic(Integer operatorId, Object function) throws InterruptedException {
		ExecutionPlanWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.updateExecutionLogic(operatorId, function);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected void defineControlAction () throws Exception{}

	protected class ControlActionRunner extends Thread{
		@Override
		public void run() {
			try {
				defineControlAction();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

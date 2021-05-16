package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationExecutor;

public class DummyController extends AbstractController {

	public DummyController(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
	}

	@Override
	public void startControllers() {
		super.controlActionRunner.start();
	}

	@Override
	public void stopControllers() {
		super.controlActionRunner.interrupt();
	}

	@Override
	protected void defineControlAction() throws Exception {
		super.defineControlAction();
		int id = super.findOperatorByName("test");
		if (id == -1) {
			System.out.println("i can not find operator with name: test");
		} else {
			System.out.println("the operator test has id: " + id);
		}
	}

}

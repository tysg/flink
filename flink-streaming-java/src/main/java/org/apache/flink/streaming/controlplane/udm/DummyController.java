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
		int id = super.findOperatorByName("Splitter FlatMap");
		if (id == -1) {
			System.out.println("I can not find operator with name: Splitter Flatmap");
		} else {
			System.out.println("The operator whose name is Splitter FlatMap has id: " + id);
		}
	}

}

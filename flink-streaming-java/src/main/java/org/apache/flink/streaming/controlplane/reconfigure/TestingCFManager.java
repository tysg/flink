package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.Iterator;
import java.util.List;

public class TestingCFManager extends ControlFunctionManager {


	public TestingCFManager(JobGraph jobGraph) {
		super(jobGraph);
		this.registerFunction(new TestingControlFunction());
	}

	@Override
	public void onJobStart() {
		super.onJobStart();

		OperatorID secondOperatorId = getSecondOperator(jobGraph);
		this.reconfigureFunction(secondOperatorId, TestingControlFunction.class);
	}

	public OperatorID getSecondOperator(JobGraph jobGraph) {
		Iterator<JobVertex> vertices = this.jobGraph.getVertices().iterator();
		boolean first = false;
		while (vertices.hasNext()) {
			JobVertex vertex = vertices.next();
			List<OperatorID> ops = vertex.getOperatorIDs();
			if (first) {
				return ops.get(0);
			}
			if (ops.size() > 1) {
				return ops.get(1);
			} else {
				first = true;
			}
		}
		return null;
	}

	private static class TestingControlFunction implements ControlFunction {

	}

}

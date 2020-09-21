package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlContext;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.reconfigure.operator.UpdatedOperatorFactory;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerService;

import java.util.Iterator;
import java.util.List;

public class TestingCFManager extends ControlFunctionManager {


	public TestingCFManager(StreamManagerService streamManagerService, JobGraphRescaler jobGraphRescaler) {
		super(streamManagerService, jobGraphRescaler);
	}

	@Override
	public void onJobStart() {
		super.onJobStart();

		JobGraph currentJobGraph = this.streamManagerService.getJobGraph();
		OperatorID secondOperatorId = getSecondOperator(currentJobGraph);
		this.reconfigure(secondOperatorId, new TestingControlFunction());
	}

	private OperatorID getSecondOperator(JobGraph jobGraph) {
		Iterator<JobVertex> vertices = jobGraph.getVertices().iterator();
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
		@Override
		public void invokeControl(ControlContext ctx, Object input) {

		}
	}

}

package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerService;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestingCFManager extends ControlFunctionManager {


	public TestingCFManager(StreamManagerService streamManagerService, JobGraphRescaler jobGraphRescaler) {
		super(streamManagerService, jobGraphRescaler);
	}

	@Override
	public void onJobStart() {
		super.onJobStart();

		JobGraph currentJobGraph = this.streamManagerService.getJobGraph();
		OperatorID secondOperatorId = findOperatorByName(currentJobGraph, "filter");
		if(secondOperatorId != null) {
			asyncRunAfter(10, () -> this.reconfigure(secondOperatorId, getFilterFunction(10)));
//			asyncRunAfter(15, () -> this.reconfigure(secondOperatorId, getFilterFunction(20)));
//			asyncRunAfter(25, () -> this.reconfigure(secondOperatorId, getFilterFunction(2)));
		}
	}

	private static ControlFunction getFilterFunction(int k) {
		return (ControlFunction) (ctx, input) -> {
			String inputWord = (String) input;
			System.out.println("now filter the words that has length smaller than " + k);
			if (inputWord.length() >= k) {
				ctx.setCurrentRes(inputWord);
			}
		};
	}


	private void asyncRunAfter(int seconds, Runnable runnable) {
		new Thread(() -> {
			try {
				Thread.sleep(seconds * 1000);
				runnable.run();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}).start();
	}


	private OperatorID findOperatorByName(JobGraph jobGraph, @Nonnull String name) {
		Iterator<JobVertex> vertices = jobGraph.getVertices().iterator();
		final ClassLoader classLoader = streamManagerService.getUserClassLoader();
		while (vertices.hasNext()) {
			JobVertex vertex = vertices.next();
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(classLoader);
			for(StreamConfig config: configMap.values()){
				if(name.equals(config.getOperatorName())){
					return config.getOperatorID();
				}
			}
		}
		return null;
	}

}

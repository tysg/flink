package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphUpdateOperator;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerService;

import java.util.List;
import java.util.Map;

public class JobGraphUpdater implements JobGraphRescaler, JobGraphUpdateOperator {

	JobGraphRescaler jobGraphRescaler;
	private JobGraph jobGraph;
	private ClassLoader userClassLoader;

	public JobGraphUpdater(JobGraphRescaler jobGraphRescaler, StreamManagerService streamManagerService) {
		this.jobGraphRescaler = jobGraphRescaler;
		this.jobGraph = streamManagerService.getJobGraph();
		this.userClassLoader = streamManagerService.getUserClassLoader();
	}

	@Override
	public Tuple2<List<JobVertexID>, List<JobVertexID>> rescale(JobVertexID id, int newParallelism, Map<Integer, List<Integer>> partitionAssignment) {
		return jobGraphRescaler.rescale(id, newParallelism, partitionAssignment);
	}

	@Override
	public Tuple2<List<JobVertexID>, List<JobVertexID>> repartition(JobVertexID id, Map<Integer, List<Integer>> partitionAssignment) {
		return jobGraphRescaler.repartition(id, partitionAssignment);
	}

	@Override
	public String print(Configuration config) {
		return jobGraphRescaler.print(config);
	}

	@Override
	public <OUT> void updateOperator(OperatorID operatorID, StreamOperatorFactory<OUT> operatorFactory) throws Exception {
		boolean chained = updateOperatorConfiguration(operatorID, config -> config.setStreamOperatorFactory(operatorFactory));
		if (chained) {
			System.out.println("updated in chained operator");
		}
	}

	/**
	 * update operators' configuration ==> stream config using @code UpdateCallBack
	 *
	 * @param operatorID the id of target operator
	 * @param callBack   the call back using to update configuration
	 * @return true if updated in chained operator otherwise false
	 * @throws Exception if do not found corresponding operator
	 */
	private boolean updateOperatorConfiguration(OperatorID operatorID, UpdateCallBack callBack) throws Exception {
		for (JobVertex vertex : jobGraph.getVertices()) {
			for (OperatorID id : vertex.getOperatorIDs()) {
				if (id.equals(operatorID)) {
					if (vertex.getOperatorIDs().size() > 1) {
						// there exists chained operators in this job vertex
						return updateConfigInChainedOperators(vertex.getConfiguration(), operatorID, callBack);
					} else {
						callBack.write(new StreamConfig(vertex.getConfiguration()));
						return false;
					}
				}
			}
		}
		throw new Exception("do not found target job vertex has this operator id");
	}

	private boolean updateConfigInChainedOperators(Configuration configuration, OperatorID operatorID, UpdateCallBack callBack) throws Exception {
		StreamConfig streamConfig = new StreamConfig(configuration);
		Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigs(userClassLoader);
		for (StreamConfig config : configMap.values()) {
			if (operatorID.equals(config.getOperatorID())) {
				callBack.write(config);
				streamConfig.setTransitiveChainedTaskConfigs(configMap);
				return true;
			}
		}
		throw new Exception("do not found target stream config with this operator id");
	}

	@VisibleForTesting
	private StreamConfig findStreamConfig(OperatorID operatorID) throws Exception {
		for (JobVertex vertex : jobGraph.getVertices()) {
			for (OperatorID id : vertex.getOperatorIDs()) {
				if (id.equals(operatorID)) {
					if (vertex.getOperatorIDs().size() > 1) {
						// there exists chained operators in this job vertex
						StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
						Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigs(userClassLoader);
						for (StreamConfig config : configMap.values()) {
							if (operatorID.equals(config.getOperatorID())) {
								return config;
							}
						}
					} else {
						return new StreamConfig(vertex.getConfiguration());
					}
				}
			}
		}
		throw new Exception("do not found target stream config with this operator id");
	}

	@FunctionalInterface
	private interface UpdateCallBack {
		void write(StreamConfig config);
	}

}

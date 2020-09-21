package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphRescaler;
import org.apache.flink.streaming.controlplane.jobgraph.JobGraphUpdateOperator;
import org.apache.flink.streaming.controlplane.rescale.StreamJobGraphRescaler;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JobGraphUpdater implements JobGraphRescaler, JobGraphUpdateOperator {

	JobGraphRescaler jobGraphRescaler;
	private JobGraph jobGraph;

	public JobGraphUpdater(JobGraphRescaler jobGraphRescaler, JobGraph jobGraph){
		this.jobGraphRescaler = jobGraphRescaler;
		this.jobGraph = jobGraph;
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
	public void updateOperator(OperatorID operatorID, StreamOperatorFactory operatorFactory) throws Exception {
		Configuration configuration = getOperatorConfiguration(operatorID);
		StreamConfig streamConfig = new StreamConfig(configuration);
		streamConfig.setStreamOperatorFactory(operatorFactory);
	}

	private Configuration getOperatorConfiguration(OperatorID operatorID) throws Exception {
		for (JobVertex vertex : jobGraph.getVertices()) {
			for (OperatorID id : vertex.getOperatorIDs()) {
				if (id == operatorID) {
					return vertex.getConfiguration();
				}
			}
		}
		throw new Exception("do not found target vertex with this operator id");
	}
}

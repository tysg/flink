package org.apache.flink.runtime.rescale;

public interface RescaleActionListener {
	void processRescaleParamsWrapper(JobRescaleAction.RescaleParamsWrapper wrapper);
}

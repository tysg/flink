package org.apache.flink.runtime.controlplane.streammanager;

public class StreamManagerAddress {

	private String rpcAddress;
	private StreamManagerId streamManagerId;

	public StreamManagerAddress(String rpcAddress, StreamManagerId streamManagerId) {
		this.rpcAddress = rpcAddress;
		this.streamManagerId = streamManagerId;
	}

	public String getRpcAddress() {
		return rpcAddress;
	}

	public StreamManagerId getStreamManagerId() {
		return streamManagerId;
	}

}

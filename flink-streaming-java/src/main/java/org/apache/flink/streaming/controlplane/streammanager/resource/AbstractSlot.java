package org.apache.flink.streaming.controlplane.streammanager.resource;

public interface AbstractSlot {
	State getState();

	Resource getResource();

	String getLocation();

	String getId();

	boolean isMatchingRequirement(Resource requirement);

	enum State {
		FREE,
		ALLOCATED,
		INUSE,
	}
}


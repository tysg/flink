package org.apache.flink.runtime.controlplane.dispatcher;

/**
 * This class is temporal used to identified some unimplemented operation in control plane
 *
 * @author hya
 */
public class ControlPlaneNotImplementedException extends UnsupportedOperationException {
	public ControlPlaneNotImplementedException(String msg) {
		super(msg);
	}

	ControlPlaneNotImplementedException(String msg, Exception e) {
		super(msg, e);
	}

}

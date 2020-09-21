package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.Serializable;

public class OperatorDescriptor implements Serializable {
	TypeInformation inputType;
	TypeInformation outputType;

	OperatorDescriptor(OperatorID operatorID, JobGraph jobGraph){

	}

}

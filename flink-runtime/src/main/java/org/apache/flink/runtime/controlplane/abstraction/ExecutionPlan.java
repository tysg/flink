package org.apache.flink.runtime.controlplane.abstraction;

/**
 * todo the interface is put in Flink runtime, seems a little not natural. another solution is
 * put it on the third module (may call streammanger-common), make both our stream manager and flink runtime import this module
 *
 */
public interface ExecutionPlan extends ExecutionGraphConfig, JobGraphConfig {

}

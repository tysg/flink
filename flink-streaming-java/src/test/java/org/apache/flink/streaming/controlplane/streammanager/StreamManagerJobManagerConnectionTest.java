package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.streaming.controlplane.dispatcher.DefaultStreamManagerRunnerFactory;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerRunnerFactory;
import org.apache.flink.util.TestLogger;
import org.junit.Before;

public class StreamManagerJobManagerConnectionTest extends TestLogger {


    private static final Time TIMEOUT = Time.seconds(10L);

    private TestingRpcService rpcService;

    private JobID jobId;

    private StreamManagerRunnerFactory streamManagerRunnerFactory = DefaultStreamManagerRunnerFactory.INSTANCE;

    private SettableLeaderRetrievalService jobMasterLeaderRetrievalService;

    private StreamManager streamManager;

    private StreamManagerGateway streamManagerGateway;

    @Before
    public void setUp() throws Exception {
        rpcService = new TestingRpcService();

        jobId = new JobID();


    }

}

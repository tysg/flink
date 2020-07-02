/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.controlplane.streammanager.StreamManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.*;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * !!! Set the flag TEST_FLAG in {@link Dispatcher} to test StreamManager. !!!
 *
 * Test for the {@link StreamManager} component.
 */
public class StreamManagerDispatcherTest extends TestLogger {

	private static RpcService rpcService;

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final Time INFINITY = RpcUtils.INF_TIMEOUT;

	private static final JobID TEST_JOB_ID = new JobID();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public TestName name = new TestName();

	private JobGraph jobGraph;

	private TestingFatalErrorHandler fatalErrorHandler;

	private TestingLeaderElectionService jobMasterLeaderElectionService;

	private CountDownLatch createdJobManagerRunnerLatch;

	private Configuration configuration;

	private BlobServer blobServer;

	/** Instance under test. */
	private TestingDispatcher dispatcher;

	private TestingHighAvailabilityServices haServices;

	private HeartbeatServices heartbeatServices;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, TIMEOUT);

			rpcService = null;
		}
	}

	@Before
	public void setUp() throws Exception {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		jobGraph = new JobGraph(TEST_JOB_ID, "testJob", testVertex);

		fatalErrorHandler = new TestingFatalErrorHandler();
		heartbeatServices = new HeartbeatServices(1000L, 10000L);

		jobMasterLeaderElectionService = new TestingLeaderElectionService();

		haServices = new TestingHighAvailabilityServices();
		haServices.setJobMasterLeaderElectionService(TEST_JOB_ID, jobMasterLeaderElectionService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
		haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());

		configuration = new Configuration();

		configuration.setString(
			BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		createdJobManagerRunnerLatch = new CountDownLatch(2);
		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Nonnull
	private TestingDispatcher createAndStartDispatcher(
			HeartbeatServices heartbeatServices,
			TestingHighAvailabilityServices haServices,
			JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
		final TestingDispatcher dispatcher = new TestingDispatcherBuilder()
			.setHaServices(haServices)
			.setHeartbeatServices(heartbeatServices)
			.setJobManagerRunnerFactory(jobManagerRunnerFactory)
			.build();
		dispatcher.start();

		return dispatcher;
	}

	private class TestingDispatcherBuilder {

		private Collection<JobGraph> initialJobGraphs = Collections.emptyList();

		private HeartbeatServices heartbeatServices = StreamManagerDispatcherTest.this.heartbeatServices;

		private HighAvailabilityServices haServices = StreamManagerDispatcherTest.this.haServices;

		private JobManagerRunnerFactory jobManagerRunnerFactory = DefaultJobManagerRunnerFactory.INSTANCE;

		private JobGraphWriter jobGraphWriter = NoOpJobGraphWriter.INSTANCE;

		TestingDispatcherBuilder setHeartbeatServices(HeartbeatServices heartbeatServices) {
			this.heartbeatServices = heartbeatServices;
			return this;
		}

		TestingDispatcherBuilder setHaServices(HighAvailabilityServices haServices) {
			this.haServices = haServices;
			return this;
		}

		TestingDispatcherBuilder setInitialJobGraphs(Collection<JobGraph> initialJobGraphs) {
			this.initialJobGraphs = initialJobGraphs;
			return this;
		}

		TestingDispatcherBuilder setJobManagerRunnerFactory(JobManagerRunnerFactory jobManagerRunnerFactory) {
			this.jobManagerRunnerFactory = jobManagerRunnerFactory;
			return this;
		}

		TestingDispatcherBuilder setJobGraphWriter(JobGraphWriter jobGraphWriter) {
			this.jobGraphWriter = jobGraphWriter;
			return this;
		}

		TestingDispatcher build() throws Exception {
			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

			final MemoryArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();

			return new TestingDispatcher(
				rpcService,
				Dispatcher.DISPATCHER_NAME + '_' + name.getMethodName(),
				DispatcherId.generate(),
				initialJobGraphs,
				new DispatcherServices(
					configuration,
					haServices,
					() -> CompletableFuture.completedFuture(resourceManagerGateway),
					blobServer,
					heartbeatServices,
					archivedExecutionGraphStore,
					fatalErrorHandler,
					VoidHistoryServerArchivist.INSTANCE,
					null,
					UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
					jobGraphWriter,
					jobManagerRunnerFactory));
		}
	}

	@After
	public void tearDown() throws Exception {
		try {
			fatalErrorHandler.rethrowError();
		} finally {
			if (dispatcher != null) {
				RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
			}
		}

		if (haServices != null) {
			haServices.closeAndCleanupAllData();
		}

		if (blobServer != null) {
			blobServer.close();
		}
	}

	/**
	 * Tests that we can submit a job to the Dispatcher which then spawns a
	 * new JobManagerRunner.
	 */
	@Test
	public void testStreamManager() throws Exception {
		dispatcher = createAndStartDispatcher(heartbeatServices, haServices, new ExpectedJobIdJobManagerRunnerFactory(TEST_JOB_ID, createdJobManagerRunnerLatch));

		DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		CompletableFuture<Acknowledge> acknowledgeFuture = dispatcherGateway.submitJob(jobGraph, INFINITY);

		assertEquals(acknowledgeFuture.get(), Acknowledge.get());
	}

	private static final class ExpectedJobIdJobManagerRunnerFactory implements JobManagerRunnerFactory {

		private final JobID expectedJobId;

		private final CountDownLatch createdJobManagerRunnerLatch;

		private ExpectedJobIdJobManagerRunnerFactory(JobID expectedJobId, CountDownLatch createdJobManagerRunnerLatch) {
			this.expectedJobId = expectedJobId;
			this.createdJobManagerRunnerLatch = createdJobManagerRunnerLatch;
		}

		@Override
		public JobManagerRunner createJobManagerRunner(
				JobGraph jobGraph,
				Configuration configuration,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				JobManagerSharedServices jobManagerSharedServices,
				JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
				FatalErrorHandler fatalErrorHandler) throws Exception {
			assertEquals(expectedJobId, jobGraph.getJobID());

			createdJobManagerRunnerLatch.countDown();

			return DefaultJobManagerRunnerFactory.INSTANCE.createJobManagerRunner(
				jobGraph,
				configuration,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				jobManagerSharedServices,
				jobManagerJobMetricGroupFactory,
				fatalErrorHandler);
		}
	}

}

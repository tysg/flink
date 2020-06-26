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

package org.apache.flink.runtime.controlplane.rest;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherGateway;
import org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherRestEndpoint;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

/**
 * {@link RestEndpointFactory} which creates a {@link StreamManagerDispatcherRestEndpoint}.
 */
public enum StreamManagerRestEndpointFactory implements RestEndpointFactory<StreamManagerDispatcherGateway> {
    INSTANCE;

    @Override
    public WebMonitorEndpoint<StreamManagerDispatcherGateway> createRestEndpoint(Configuration configuration,
            LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
            LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            TransientBlobService transientBlobService, ScheduledExecutorService executor, MetricFetcher metricFetcher,
            LeaderElectionService leaderElectionService, FatalErrorHandler fatalErrorHandler) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
    
}

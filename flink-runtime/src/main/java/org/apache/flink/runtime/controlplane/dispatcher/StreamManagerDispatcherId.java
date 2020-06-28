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

package org.apache.flink.runtime.controlplane.dispatcher;

import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.util.AbstractID;

import java.util.UUID;

/**
 * Fencing token of the {@link StreamManagerDispatcher}.
 */

public class StreamManagerDispatcherId extends AbstractID {

	private static final long serialVersionUID = 1L; // TODO: generate real serialVersionUID with plugin

	private StreamManagerDispatcherId() {}

	private StreamManagerDispatcherId(UUID uuid) {
		super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
	}

	public UUID toUUID() {
		return new UUID(getUpperPart(), getLowerPart());
	}

	/**
	 * Generates a new random DispatcherId.
	 */
	public static org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherId generate() {
		return new org.apache.flink.runtime.controlplane.dispatcher.StreamManagerDispatcherId();
	}

	/**
	 * Creates a new DispatcherId that corresponds to the UUID.
	 */
	public static StreamManagerDispatcherId fromUuid(UUID uuid) {
		return new StreamManagerDispatcherId(uuid);
	}
}

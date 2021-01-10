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

package org.apache.flink.runtime.util.profiling;

import static org.apache.flink.util.Preconditions.checkState;

public class ReconfigurationProfiler {
	// TODO: add breakdown profiling metrics such as sync time, update time, etc.
	private final Timer endToEndTimer;
	private final Timer syncTimer;
	private final Timer updateTimer;

	public ReconfigurationProfiler() {
		endToEndTimer = new Timer("endToEndTimer");
		syncTimer = new Timer("syncTimer");
		updateTimer = new Timer("updateTimer");
	}

	public void onReconfigurationStart() {
		endToEndTimer.startMeasure();
	}

	public void onSyncStart() {
		syncTimer.startMeasure();
	}

	public void onUpdateStart() {
		updateTimer.startMeasure();
	}

	public void onReconfigurationEnd() {
		endToEndTimer.endMeasure();
	}

	public void onUpdateEnd() {
		updateTimer.endMeasure();
	}

	private static class Timer {
		private final String timerName;
		private long startTime;

		Timer(String timerName) {
			this.timerName = timerName;
			this.startTime = 0;
		}

		public void startMeasure () {
			System.out.println("cur time: " + System.currentTimeMillis());
			startTime = System.currentTimeMillis();
		}

		public void endMeasure () {
			checkState(startTime > 0, "++++++ Invalid invocation, startTime = 0.");
			System.out.println("end time: " + System.currentTimeMillis());
			System.out.println("++++++" + timerName + " : " + (System.currentTimeMillis() - startTime));
		}
	}
}



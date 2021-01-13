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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

public class ReconfigurationProfiler {

	public static final String UPDATE_STATE = "update_state";
	public static final String UPDATE_KEY_MAPPING = "update_key_mapping";
	public static final String UPDATE_RESOURCE = "update_resource";

	// TODO: add breakdown profiling metrics such as sync time, update time, etc.
	private final Timer endToEndTimer;
	private final Timer syncTimer;
	private final Timer updateTimer;

	private final Map<String,Timer> otherTimers;

	public ReconfigurationProfiler() {
		OutputStream outputStream;
		try {
			outputStream = new FileOutputStream("/home/hya/prog/exp.output");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			outputStream = System.out;
		}
		endToEndTimer = new Timer("endToEndTimer", outputStream);
		syncTimer = new Timer("syncTimer");
		updateTimer = new Timer("updateTimer");
		otherTimers = new HashMap<>();
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

	public void onOtherStart(String timerName){
		Timer timer = this.otherTimers.get(timerName);
		if(timer == null){
			timer = new Timer(timerName);
			otherTimers.put(timerName, timer);
		}
		timer.startMeasure();
	}

	public void onOtherEnd(String timerName){
		this.otherTimers.get(timerName).endMeasure();
	}

	private static class Timer {
		private final String timerName;
		private long startTime;
		private final LikePrintOutputStream outputStream;

		Timer(String timerName) {
			this(timerName, System.out);
		}

		Timer(String timerName, OutputStream outputStream) {
			this.timerName = timerName;
			this.startTime = 0;
			this.outputStream = new LikePrintOutputStream(outputStream);
		}

		public void startMeasure () {
			outputStream.println("cur time: " + System.currentTimeMillis());
			startTime = System.currentTimeMillis();
		}

		public void endMeasure () {
			checkState(startTime > 0, "++++++ Invalid invocation, startTime = 0.");
			outputStream.println("end time: " + System.currentTimeMillis());
			outputStream.println("++++++" + timerName + " : " + (System.currentTimeMillis() - startTime) + "ms");
		}
	}

	private static class LikePrintOutputStream{

		private final OutputStream outputStream;

		private LikePrintOutputStream(OutputStream outputStream) {
			this.outputStream = outputStream;
		}

		void println(String s){
			try {
				outputStream.write(s.getBytes());
				outputStream.write('\n');
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}



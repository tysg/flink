package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.Task;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class TaskOperatorManager {

	private final PauseActionController pauseActionController;
	private final Task containedTask;

	public TaskOperatorManager(Task task) {
		this.containedTask = task;
		this.pauseActionController = new PauseActionControllerImpl();
	}

	public PauseActionController getPauseActionController() {
		return pauseActionController;
	}

	@ThreadSafe
	public interface PauseActionController {

		/**
		 * This method should be thread safe.
		 * called by task process thread, should avoid acquire lock as possible as we can
		 * since task will check this method each time process an input.
		 *
		 * @return true if task should be paused
		 */
		boolean ackIfPause();

		CompletableFuture<Acknowledge> getResumeFuture();

		/**
		 * This method should be thread safe.
		 * called by none task process thread
		 *
		 * @return A future to wait for acknowledgment of task process thread
		 * @throws Exception
		 */
		CompletableFuture<Acknowledge> setPausedAndGetAckFuture() throws Exception;

		void resume() throws Exception;
	}

	public static class PauseActionControllerImpl implements PauseActionController {

		private CompletableFuture<Acknowledge> resumeFuture;
		private CompletableFuture<Acknowledge> ackPausedFuture;
		private final AtomicReference<TaskStatus> state = new AtomicReference<>(TaskStatus.READY);

		private final Object lock = new Object();

		@Override
		public boolean ackIfPause() {
			if (state.get() == TaskStatus.PAUSE) {
				// only the state become pause should we acquire the lock
				synchronized (lock) {
					ackPausedFuture.complete(Acknowledge.get());
					return true;
				}
			}
			return false;
		}

		@Override
		public void resume() throws Exception {
			if (state.compareAndSet(TaskStatus.PAUSE, TaskStatus.READY)) {
				resumeFuture.complete(Acknowledge.get());
			} else {
				throw new Exception("state has been ready before");
			}
		}

		@Override
		public CompletableFuture<Acknowledge> setPausedAndGetAckFuture() throws Exception {
			synchronized (lock) {
				if (state.compareAndSet(TaskStatus.READY, TaskStatus.PAUSE)) {
					ackPausedFuture = new CompletableFuture<>();
					resumeFuture = new CompletableFuture<>();
					return ackPausedFuture;
				} else {
					throw new Exception("state has been paused before");
				}
			}
		}

		@Override
		public CompletableFuture<Acknowledge> getResumeFuture() {
			return resumeFuture;
		}

		private enum TaskStatus {
			PAUSE,
			READY
		}
	}

	@VisibleForTesting
	public static class NoControlImpl implements PauseActionController {

		@Override
		public boolean ackIfPause() {
			return false;
		}

		@Override
		public CompletableFuture<Acknowledge> getResumeFuture() {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public CompletableFuture<Acknowledge> setPausedAndGetAckFuture() throws Exception {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public void resume() throws Exception {

		}
	}

}

package org.pinus.datalayer.jdbc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.pinus.api.ITask;
import org.pinus.api.TaskProgress;
import org.pinus.api.query.IQuery;

/**
 * 数据处理执行器.
 * 
 * @author duanbn
 *
 */
public class TaskExecutor {

	private Class<?> clazz;

	public TaskExecutor(Class<?> clazz) {
		this.clazz = clazz;
	}

	public Future<TaskProgress> execute(ITask<?> task) {
		return execute(task, null);
	}

	public Future<TaskProgress> execute(ITask<?> task, IQuery query) {
		TaskFuture future = new TaskFuture();

		return future;
	}

	public static class TaskFuture implements Future<TaskProgress> {

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isCancelled() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isDone() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public TaskProgress get() throws InterruptedException, ExecutionException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TaskProgress get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
				TimeoutException {
			throw new UnsupportedOperationException();
		}

	}

}

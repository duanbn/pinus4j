package org.pinus.datalayer.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.pinus.api.ITask;
import org.pinus.api.TaskFuture;
import org.pinus.api.query.IQuery;
import org.pinus.cluster.DB;
import org.pinus.cluster.IDBCluster;
import org.pinus.cluster.beans.DBConnectionInfo;
import org.pinus.datalayer.IRecordReader;
import org.pinus.datalayer.iterator.GlobalRecordIterator;
import org.pinus.datalayer.iterator.ShardingRecordIterator;
import org.pinus.exception.DBClusterException;
import org.pinus.exception.DBOperationException;
import org.pinus.util.ReflectUtil;
import org.pinus.util.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据处理执行器.
 * 
 * @author duanbn
 *
 */
public class TaskExecutor<E> {

	public static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

	/**
	 * 处理线程池名称.
	 */
	private static final String THREADPOOL_NAME = "pinus";

	/**
	 * 本次处理的数据对象
	 */
	private Class<E> clazz;

	/**
	 * 数据库集群引用
	 */
	private IDBCluster dbCluster;

	public TaskExecutor(Class<E> clazz, IDBCluster dbCluster) {
		this.clazz = clazz;

		this.dbCluster = dbCluster;
	}

	public TaskFuture execute(ITask<E> task) {
		return execute(task, null);
	}

	public TaskFuture execute(ITask<E> task, IQuery query) {
		// 创建线程池.
		ThreadPool threadPool = ThreadPool.newInstance(THREADPOOL_NAME);

		TaskFuture future = null;

		String clusterName = ReflectUtil.getClusterName(clazz);

		IRecordReader<E> reader = null;
		if (ReflectUtil.isShardingEntity(clazz)) {
			List<DB> dbs = this.dbCluster.getAllMasterShardingDB(clazz);

			List<IRecordReader<E>> readers = new ArrayList<IRecordReader<E>>(dbs.size());

			// 计算总数
			long total = 0;
			for (DB db : dbs) {
				reader = new ShardingRecordIterator<E>(db, clazz);
				reader.setQuery(query);
				readers.add(reader);
				total += reader.getCount();
			}

			future = new TaskFuture(total, threadPool);

			for (IRecordReader<E> r : readers) {
				threadPool.submit(new RecrodReaderThread<E>(r, threadPool, task, future));
			}
		} else {
			RecrodThread<E> rt = null;

			DBConnectionInfo dbConnInfo;
			try {
				dbConnInfo = this.dbCluster.getMasterGlobalConn(clusterName);
			} catch (DBClusterException e) {
				throw new DBOperationException(e);
			}
			reader = new GlobalRecordIterator<E>(dbConnInfo, clazz);
			reader.setQuery(query);

			future = new TaskFuture(reader.getCount(), threadPool);

			while (reader.hasNext()) {
				E record = reader.next();
				rt = new RecrodThread<E>(record, task, future);
				threadPool.submit(rt);
			}
		}

		return future;
	}

	public static class RecrodReaderThread<E> implements Runnable {

		private IRecordReader<E> recordReader;

		private ThreadPool threadPool;

		private ITask<E> task;

		private TaskFuture future;

		public RecrodReaderThread(IRecordReader<E> recordReader, ThreadPool threadPool, ITask<E> task, TaskFuture future) {
			this.recordReader = recordReader;
			this.threadPool = threadPool;
			this.task = task;
			this.future = future;
		}

		@Override
		public void run() {
			RecrodThread<E> rt = null;
			while (recordReader.hasNext()) {
				E record = recordReader.next();
				rt = new RecrodThread<E>(record, task, future);
				threadPool.submit(rt);
			}
		}

	}

	public static class RecrodThread<E> implements Runnable {

		public static final Logger LOG = LoggerFactory.getLogger(RecrodThread.class);

		private E record;

		private ITask<E> task;

		private TaskFuture future;

		public RecrodThread(E record, ITask<E> task, TaskFuture future) {
			this.record = record;
			this.task = task;
			this.future = future;
		}

		@Override
		public void run() {
			try {
				this.task.doTask(record);
			} catch (Exception e) {
				LOG.warn("do task failure " + record);
			} finally {
				this.future.down();
				this.future.incrCount();
			}
		}

	}

}

package org.pinus4j.api;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.exceptions.TaskException;
import org.pinus4j.utils.ThreadPool;

/**
 * 数据处理任务进度.
 * 
 * @author duanbn
 *
 */
public class TaskFuture {

	/**
	 * 需要处理的总记录数
	 */
	private long total;

	/**
	 * 阻塞调用线程.
	 */
	private CountDownLatch cdl;

	/**
	 * 已经处理的记录数
	 */
	private AtomicLong count = new AtomicLong(0);

	/**
	 * 执行处理的线程池
	 */
	private ThreadPool threadPool;

	/**
	 * 被执行的任务
	 */
	private ITask<?> task;

	private List<IDBResource> dbResources = new ArrayList<IDBResource>();

	public TaskFuture(long total, ThreadPool threadPool, ITask<?> task) {
		this.total = total;

		this.cdl = new CountDownLatch((int) total);

		this.threadPool = threadPool;

		// 当前执行的任务引用.
		this.task = task;
	}

	public String getProgress() {
		double val = new Long(this.count.get()).doubleValue() / new Long(this.total).doubleValue();
		DecimalFormat df = new DecimalFormat("0.00");
		return df.format(val);
	}

	/**
	 * 当然任务是否已经完成
	 * 
	 * @return
	 */
	public boolean isDone() {
		return this.count.get() == this.total;
	}

	public void await() throws InterruptedException {
		try {
			// 线程阻塞，当本次处理完成之后会取消阻塞
			this.cdl.await();
		} catch (Exception e) {
			throw new TaskException(e);
		}
	}

	public void down(int count) {
		for (int i = 0; i < count; i++)
			this.cdl.countDown();
		if (isDone()) {
			try {
				this.task.finish();
			} catch (Exception e) {
				throw new TaskException(e);
			} finally {
				// 关闭线程池
				this.threadPool.shutdown();
				// release db connection
				for (IDBResource dbResource : dbResources) {
					dbResource.close();
				}
			}
		}
	}

	public void incrCount(int count) {
		if (this.count.get() < this.total)
			this.count.addAndGet(count);
	}

	public long getTotal() {
		return total;
	}

	public long getCount() {
		return this.count.get();
	}

	public void addDBResource(IDBResource dbResource) {
		this.dbResources.add(dbResource);
	}

	public void addDBResource(List<IDBResource> dbResources) {
		this.dbResources.addAll(dbResources);
	}

	@Override
	public String toString() {
		return "TaskFuture [total=" + total + ", cdl=" + cdl + ", count=" + count + "]";
	}

}

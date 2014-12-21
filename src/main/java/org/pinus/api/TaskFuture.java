package org.pinus.api;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.pinus.util.ThreadPool;

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
	private AtomicLong count = new AtomicLong(1);

	/**
	 * 执行处理的线程池
	 */
	private ThreadPool threadPool;

	public TaskFuture(long total, ThreadPool threadPool) {
		this.total = total;

		this.cdl = new CountDownLatch((int) total);

		this.threadPool = threadPool;
	}

	public void await() throws InterruptedException {
		try {
			this.cdl.await();
		} finally {
			// 关闭线程池
			this.threadPool.shutdown();
		}
	}

	public void down() {
		this.cdl.countDown();
	}

	public void incrCount() {
		if (this.count.get() < this.total)
			this.count.incrementAndGet();
	}

	public long getTotal() {
		return total;
	}

	public long getCount() {
		return this.count.get();
	}

	@Override
	public String toString() {
		return "TaskFuture [total=" + total + ", cdl=" + cdl + ", count="
				+ count + "]";
	}

}

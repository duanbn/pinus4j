/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.utils;

import java.util.concurrent.atomic.*;
import java.util.concurrent.*;

/**
 * 线程池. 每次newInstance时会创建一个新的线程池实例，当不在使用时需要调用shutdown方法关闭线程池.
 *
 * @author duanbn
 */
public class ThreadPool {

	private ThreadPoolExecutor pool = null;

	private static final int QUEUE_SIZE = 50000;

	private ThreadPool(String poolName, int minThread, int maxThread,
			long waitTime) {
		pool = new ThreadPoolExecutor(minThread, maxThread, waitTime,
				TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(
						getQueueSize()), new NameThreadFactory(poolName),
				new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public static ThreadPool newInstance(String poolName) {
		int minThread = Runtime.getRuntime().availableProcessors() * 2;
		int maxThread = minThread * 4;
		long waitTime = 3 * 1000;

		return newInstance(poolName, minThread, maxThread, waitTime);
	}

	public static ThreadPool newInstance(String poolName, int minThread,
			int maxThread, long waitTime) {
		return new ThreadPool(poolName, minThread, maxThread, waitTime);
	}

	public ThreadPoolExecutor getThreadPool() {
		return pool;
	}

	public int getQueueSize() {
		return QUEUE_SIZE;
	}

	public boolean isShutdown() {
		return pool.isShutdown();
	}

	public void shutdown() {
		pool.shutdown();
	}

	public void execute(Runnable r) {
		pool.execute(r);
	}

	public Future<?> submit(Runnable r) {
		return pool.submit(r);
	}

	private class NameThreadFactory implements ThreadFactory {
		private final ThreadGroup group;
		private final AtomicInteger threadNumber = new AtomicInteger(1);
		private final String namePrefix;

		public NameThreadFactory(String poolName) {
			SecurityManager s = System.getSecurityManager();
			this.group = (s != null) ? s.getThreadGroup() : Thread
					.currentThread().getThreadGroup();
			this.namePrefix = "pool-" + poolName + "-thread-";
		}

		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r, namePrefix
					+ threadNumber.getAndIncrement(), 0);
			if (t.isDaemon())
				t.setDaemon(false);
			if (t.getPriority() != Thread.NORM_PRIORITY)
				t.setPriority(Thread.NORM_PRIORITY);
			return t;
		}
	}

}

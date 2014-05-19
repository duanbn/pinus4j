package com.pinus.cluster;

import com.pinus.cluster.lock.DistributedLock;

public class DistributedLockTest {

	private static int count;

	public static void main(String... args) throws Exception {
		for (int j = 0; j < 18; j++) {
			Counter c = new Counter();
			c.start();
		}

		Thread.sleep(2000);
	}

	public static class Counter extends Thread {
		@Override
		public void run() {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				DistributedLock lock = new DistributedLock("test", false);
				try {
					lock.lock();
					System.out.println(count++);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					lock.unlock();
				}
			}
		}
	}

}

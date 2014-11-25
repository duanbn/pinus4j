package com.pinus.cluster.lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public class CuratorDistributeedLock implements Lock {

	private InterProcessMutex curatorLock;

	public CuratorDistributeedLock(InterProcessMutex curatorLock) {
		this.curatorLock = curatorLock;
	}

	@Override
	public void lock() {
		try {
			this.curatorLock.acquire();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tryLock() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		try {
			return this.curatorLock.acquire(time, unit);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void unlock() {
		try {
			this.curatorLock.release();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException();
		}
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException();
	}

}

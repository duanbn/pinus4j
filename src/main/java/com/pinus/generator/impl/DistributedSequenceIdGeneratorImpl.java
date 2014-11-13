package com.pinus.generator.impl;

import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;

import com.pinus.cluster.lock.CuratorDistributeedLock;
import com.pinus.cluster.lock.DistributedLock;
import com.pinus.config.IClusterConfig;
import com.pinus.generator.AbstractSequenceIdGenerator;

/**
 * 分布式模式ID生成器. 在分布式情况下，主键生成需要依赖于一个分布式锁来保证ID生成的顺序，不会出现重复ID的情况.
 * 
 * @author duanbn
 */
public class DistributedSequenceIdGeneratorImpl extends AbstractSequenceIdGenerator {

	/**
	 * 日志.
	 */
	public static Logger LOG = Logger.getLogger(DistributedSequenceIdGeneratorImpl.class);

	private CuratorFramework curatorClient;

	public DistributedSequenceIdGeneratorImpl(IClusterConfig config, CuratorFramework curatorClient) {
		super(config);
		this.curatorClient = curatorClient;
	}

	@Override
	public Lock getLock(String lockName) {
		InterProcessMutex curatorLock = new InterProcessMutex(curatorClient, "/curatorlocks/" + lockName);
		return new CuratorDistributeedLock(curatorLock);
	}

}

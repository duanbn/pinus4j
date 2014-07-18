package com.pinus.generator.impl;

import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

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

	private IClusterConfig config;

	public DistributedSequenceIdGeneratorImpl(IClusterConfig config) {
		super(config);
		this.config = config;
	}

	@Override
	public Lock getLock(String lockName) {
		return new DistributedLock(lockName, true, config);
	}

}

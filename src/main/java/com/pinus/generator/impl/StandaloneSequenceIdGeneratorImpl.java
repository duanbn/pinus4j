package com.pinus.generator.impl;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.pinus.generator.AbstractSequenceIdGenerator;

/**
 * 基于零号库生成的集群全局唯一id实现. 此实现是一种简单的实现，基于一个集群的零号库生成主键策略.
 * 
 * @author duanbn
 */
public class StandaloneSequenceIdGeneratorImpl extends AbstractSequenceIdGenerator {

	/**
	 * 日志.
	 */
	public static Logger LOG = Logger.getLogger(StandaloneSequenceIdGeneratorImpl.class);

	/**
	 * 单机锁. 当使用单机模式运行的时候需要防止多线程修改global_id表
	 */
	private static final ReentrantLock standaloneLock = new ReentrantLock();

	@Override
	public Lock getLock(String lockName) {
		return standaloneLock;
	}

}

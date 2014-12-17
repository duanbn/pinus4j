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

package org.pinus.generator.impl;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.pinus.config.IClusterConfig;
import org.pinus.generator.AbstractSequenceIdGenerator;

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

	public StandaloneSequenceIdGeneratorImpl(IClusterConfig config) {
		super(config);
	}

	@Override
	public Lock getLock(String lockName) {
		return standaloneLock;
	}

}

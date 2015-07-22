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

package org.pinus4j.generator.impl;

import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.constant.Const;
import org.pinus4j.generator.AbstractSequenceIdGenerator;
import org.pinus4j.utils.CuratorDistributeedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式模式ID生成器. 在分布式情况下，主键生成需要依赖于一个分布式锁来保证ID生成的顺序，不会出现重复ID的情况.
 * 
 * @author duanbn
 */
public class DistributedSequenceIdGeneratorImpl extends AbstractSequenceIdGenerator {

	/**
	 * 日志.
	 */
	public static Logger LOG = LoggerFactory.getLogger(DistributedSequenceIdGeneratorImpl.class);

	private CuratorFramework curatorClient;

	public DistributedSequenceIdGeneratorImpl(IClusterConfig config, CuratorFramework curatorClient) {
		super(curatorClient, config.getIdGeneratorBatch());
		this.curatorClient = curatorClient;
	}

	@Override
	public Lock getLock(String lockName) {
		InterProcessMutex curatorLock = new InterProcessMutex(curatorClient, Const.ZK_LOCKS + "/" + lockName);
		return new CuratorDistributeedLock(curatorLock);
	}

}

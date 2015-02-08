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

package org.pinus4j.datalayer.jdbc;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.datalayer.IDataLayerBuilder;
import org.pinus4j.datalayer.IGlobalMasterQuery;
import org.pinus4j.datalayer.IGlobalSlaveQuery;
import org.pinus4j.datalayer.IGlobalUpdate;
import org.pinus4j.datalayer.IShardingMasterQuery;
import org.pinus4j.datalayer.IShardingSlaveQuery;
import org.pinus4j.datalayer.IShardingUpdate;
import org.pinus4j.generator.IIdGenerator;

/**
 * default builder for datalayer component.
 *
 * @author duanbn
 * @since 0.7.1
 */
public class JdbcDataLayerBuilder implements IDataLayerBuilder {

	private IDBCluster dbCluster;

	private IPrimaryCache primaryCache;

	private ISecondCache secondCache;

	private static JdbcDataLayerBuilder instance;

	private JdbcDataLayerBuilder() {
	}

	public static IDataLayerBuilder valueOf(IDBCluster dbCluster) {
		if (instance == null) {
			synchronized (JdbcDataLayerBuilder.class) {
				if (instance == null) {
					instance = new JdbcDataLayerBuilder();
					instance.setDBCluster(dbCluster);
				}
			}
		}

		return instance;
	}

	public void setDBCluster(IDBCluster dbCluster) {
		if (dbCluster == null) {
			throw new IllegalArgumentException("input param should not be null");
		}

		this.dbCluster = dbCluster;
	}

	@Override
	public IDataLayerBuilder setPrimaryCache(IPrimaryCache primaryCache) {
		if (this.primaryCache != null)
			this.primaryCache = primaryCache;
		return this;
	}

	@Override
	public IDataLayerBuilder setSecondCache(ISecondCache secondCache) {
		if (this.secondCache != null)
			this.secondCache = secondCache;
		return this;
	}

	@Override
	public IGlobalUpdate buildGlobalUpdate(IIdGenerator idGenerator) {
		GlobalJdbcUpdateImpl globalUpdate = new GlobalJdbcUpdateImpl();
		globalUpdate.setTransactionManager(this.dbCluster.getTransactionManager());
		globalUpdate.setIdGenerator(idGenerator);
		globalUpdate.setDBCluster(this.dbCluster);
		globalUpdate.setPrimaryCache(this.primaryCache);
		globalUpdate.setSecondCache(this.secondCache);
		return globalUpdate;
	}

	@Override
	public IGlobalMasterQuery buildGlobalMasterQuery() {
		GlobalJdbcMasterQueryImpl globalMasterQuery = new GlobalJdbcMasterQueryImpl();
		globalMasterQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		globalMasterQuery.setDBCluster(this.dbCluster);
		globalMasterQuery.setPrimaryCache(this.primaryCache);
		globalMasterQuery.setSecondCache(this.secondCache);
		return globalMasterQuery;
	}

	@Override
	public IGlobalSlaveQuery buildGlobalSlaveQuery() {
		GlobalJdbcSlaveQueryImpl globalSlaveQuery = new GlobalJdbcSlaveQueryImpl();
		globalSlaveQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		globalSlaveQuery.setDBCluster(this.dbCluster);
		globalSlaveQuery.setPrimaryCache(this.primaryCache);
		globalSlaveQuery.setSecondCache(this.secondCache);
		return globalSlaveQuery;
	}

	@Override
	public IShardingUpdate buildShardingUpdate(IIdGenerator idGenerator) {
		ShardingJdbcUpdateImpl shardingUpdate = new ShardingJdbcUpdateImpl();
		shardingUpdate.setTransactionManager(this.dbCluster.getTransactionManager());
		shardingUpdate.setIdGenerator(idGenerator);
		shardingUpdate.setDBCluster(this.dbCluster);
		shardingUpdate.setPrimaryCache(this.primaryCache);
		shardingUpdate.setSecondCache(this.secondCache);
		return shardingUpdate;
	}

	@Override
	public IShardingMasterQuery buildShardingMasterQuery() {
		ShardingJdbcMasterQueryImpl shardingMasterQuery = new ShardingJdbcMasterQueryImpl();
		shardingMasterQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		shardingMasterQuery.setDBCluster(this.dbCluster);
		shardingMasterQuery.setPrimaryCache(this.primaryCache);
		shardingMasterQuery.setSecondCache(this.secondCache);
		return shardingMasterQuery;
	}

	@Override
	public IShardingSlaveQuery buildShardingSlaveQuery() {
		ShardingJdbcSlaveQueryImpl shardingSlaveQuery = new ShardingJdbcSlaveQueryImpl();
		shardingSlaveQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		shardingSlaveQuery.setDBCluster(this.dbCluster);
		shardingSlaveQuery.setPrimaryCache(this.primaryCache);
		shardingSlaveQuery.setSecondCache(this.secondCache);
		return shardingSlaveQuery;
	}

}

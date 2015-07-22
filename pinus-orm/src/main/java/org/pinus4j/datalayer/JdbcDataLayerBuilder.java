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

package org.pinus4j.datalayer;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.datalayer.query.IGlobalMasterQuery;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.datalayer.query.IGlobalSlaveQuery;
import org.pinus4j.datalayer.query.IShardingMasterQuery;
import org.pinus4j.datalayer.query.IShardingQuery;
import org.pinus4j.datalayer.query.IShardingSlaveQuery;
import org.pinus4j.datalayer.query.jdbc.GlobalJdbcMasterQueryImpl;
import org.pinus4j.datalayer.query.jdbc.GlobalJdbcQueryImpl;
import org.pinus4j.datalayer.query.jdbc.GlobalJdbcSlaveQueryImpl;
import org.pinus4j.datalayer.query.jdbc.ShardingJdbcMasterQueryImpl;
import org.pinus4j.datalayer.query.jdbc.ShardingJdbcQueryImpl;
import org.pinus4j.datalayer.query.jdbc.ShardingJdbcSlaveQueryImpl;
import org.pinus4j.datalayer.update.IGlobalUpdate;
import org.pinus4j.datalayer.update.IShardingUpdate;
import org.pinus4j.datalayer.update.jdbc.GlobalJdbcUpdateImpl;
import org.pinus4j.datalayer.update.jdbc.ShardingJdbcUpdateImpl;
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

	private static volatile JdbcDataLayerBuilder instance;

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
		if (this.primaryCache == null)
			this.primaryCache = primaryCache;
		return this;
	}

	@Override
	public IDataLayerBuilder setSecondCache(ISecondCache secondCache) {
		if (this.secondCache == null)
			this.secondCache = secondCache;
		return this;
	}
	
	@Override
	public IGlobalQuery buildGlobalQuery() {
		GlobalJdbcQueryImpl globalQuery = new GlobalJdbcQueryImpl();
		globalQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		globalQuery.setDBCluster(this.dbCluster);
		globalQuery.setPrimaryCache(this.primaryCache);
		globalQuery.setSecondCache(this.secondCache);
		return globalQuery;
	}

	@Override
	public IShardingQuery buildShardingQuery() {
		ShardingJdbcQueryImpl shardingQuery = new ShardingJdbcQueryImpl();
		shardingQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		shardingQuery.setDBCluster(this.dbCluster);
		shardingQuery.setPrimaryCache(this.primaryCache);
		shardingQuery.setSecondCache(this.secondCache);
		return shardingQuery;
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

	@Deprecated
	@Override
	public IGlobalMasterQuery buildGlobalMasterQuery() {
		GlobalJdbcMasterQueryImpl globalMasterQuery = new GlobalJdbcMasterQueryImpl();
		globalMasterQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		globalMasterQuery.setDBCluster(this.dbCluster);
		globalMasterQuery.setPrimaryCache(this.primaryCache);
		globalMasterQuery.setSecondCache(this.secondCache);
		return globalMasterQuery;
	}

	@Deprecated
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

	@Deprecated
	@Override
	public IShardingMasterQuery buildShardingMasterQuery() {
		ShardingJdbcMasterQueryImpl shardingMasterQuery = new ShardingJdbcMasterQueryImpl();
		shardingMasterQuery.setTransactionManager(this.dbCluster.getTransactionManager());
		shardingMasterQuery.setDBCluster(this.dbCluster);
		shardingMasterQuery.setPrimaryCache(this.primaryCache);
		shardingMasterQuery.setSecondCache(this.secondCache);
		return shardingMasterQuery;
	}

	@Deprecated
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

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

package org.pinus.datalayer.jdbc;

import org.pinus.cache.IPrimaryCache;
import org.pinus.cache.ISecondCache;
import org.pinus.cluster.IDBCluster;
import org.pinus.datalayer.IDataLayerBuilder;
import org.pinus.datalayer.IGlobalMasterQuery;
import org.pinus.datalayer.IGlobalSlaveQuery;
import org.pinus.datalayer.IGlobalUpdate;
import org.pinus.datalayer.IShardingMasterQuery;
import org.pinus.datalayer.IShardingSlaveQuery;
import org.pinus.datalayer.IShardingUpdate;
import org.pinus.generator.IIdGenerator;

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
	public void reset() {
		this.dbCluster = null;
		this.primaryCache = null;
		this.secondCache = null;
	}

	@Override
	public IGlobalUpdate buildGlobalUpdate(IIdGenerator idGenerator) {
		IGlobalUpdate globalUpdate = new GlobalJdbcUpdateImpl();
		globalUpdate.setIdGenerator(idGenerator);
		globalUpdate.setDBCluster(this.dbCluster);
		globalUpdate.setPrimaryCache(this.primaryCache);
		globalUpdate.setSecondCache(this.secondCache);
		return globalUpdate;
	}

	@Override
	public IGlobalMasterQuery buildGlobalMasterQuery() {
		IGlobalMasterQuery globalMasterQuery = new GlobalJdbcMasterQueryImpl();
		globalMasterQuery.setDBCluster(this.dbCluster);
		globalMasterQuery.setPrimaryCache(this.primaryCache);
		globalMasterQuery.setSecondCache(this.secondCache);
		return globalMasterQuery;
	}

	@Override
	public IGlobalSlaveQuery buildGlobalSlaveQuery() {
		IGlobalSlaveQuery globalSlaveQuery = new GlobalJdbcSlaveQueryImpl();
		globalSlaveQuery.setDBCluster(this.dbCluster);
		globalSlaveQuery.setPrimaryCache(this.primaryCache);
		globalSlaveQuery.setSecondCache(this.secondCache);
		return globalSlaveQuery;
	}

	@Override
	public IShardingUpdate buildShardingUpdate(IIdGenerator idGenerator) {
		IShardingUpdate shardingUpdate = new ShardingJdbcUpdateImpl();
		shardingUpdate.setIdGenerator(idGenerator);
		shardingUpdate.setDBCluster(this.dbCluster);
		shardingUpdate.setPrimaryCache(this.primaryCache);
		shardingUpdate.setSecondCache(this.secondCache);
		return shardingUpdate;
	}

	@Override
	public IShardingMasterQuery buildShardingMasterQuery() {
		IShardingMasterQuery shardingMasterQuery = new ShardingJdbcMasterQueryImpl();
		shardingMasterQuery.setDBCluster(this.dbCluster);
		shardingMasterQuery.setPrimaryCache(this.primaryCache);
		shardingMasterQuery.setSecondCache(this.secondCache);
		return shardingMasterQuery;
	}

	@Override
	public IShardingSlaveQuery buildShardingSlaveQuery() {
		IShardingSlaveQuery shardingSlaveQuery = new ShardingJdbcSlaveQueryImpl();
		shardingSlaveQuery.setDBCluster(this.dbCluster);
		shardingSlaveQuery.setPrimaryCache(this.primaryCache);
		shardingSlaveQuery.setSecondCache(this.secondCache);
		return shardingSlaveQuery;
	}

}

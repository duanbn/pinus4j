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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pinus4j.api.IShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.IShardingUpdate;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.ReflectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分库分表更新实现. 更新操作包括插入、删除、更新，这些操作只操作主库.
 * 
 * @author duanbn
 */
public class ShardingJdbcUpdateImpl extends AbstractJdbcUpdate implements IShardingUpdate {

	/**
	 * 日志.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(ShardingJdbcUpdateImpl.class);

	@SuppressWarnings({ "rawtypes" })
	@Override
	public Number save(Object entity, IShardingKey shardingKey) {
		Class clazz = entity.getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		boolean isCheckPrimaryKey = true;
		// set primary key.
		Number pk = ReflectUtil.getPkValue(entity);
		if (pk == null || pk.intValue() == 0) {
			isCheckPrimaryKey = false;
			pk = this.idGenerator.genClusterUniqueLongId(Const.ZK_PRIMARYKEY + "/" + shardingKey.getClusterName(),
					tableName);
			try {
				ReflectUtil.setPkValue(entity, pk);
			} catch (Exception e) {
				throw new DBOperationException(e);
			}
		}

		if (isCheckPrimaryKey)
			this.idGenerator.checkAndSetPrimaryKey(pk.longValue(), shardingKey.getClusterName(), tableName);

		ShardingDBResource db = _getDbFromMaster(tableName, shardingKey);

		List<Object> entities = new ArrayList<Object>(1);
		entities.add(entity);
		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_saveBatch(conn, entities, db.getTableIndex());

			if (isCacheAvailable(clazz)) {
				primaryCache.incrCount(db, 1);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return pk;
	}

	@Override
	public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		Class<?> clazz = entities.get(0).getClass();
		String tableName = ReflectUtil.getTableName(clazz);

		ShardingDBResource db = _getDbFromMaster(tableName, shardingKey);

		int entitySize = entities.size();
		Number[] pks = new Number[entitySize];
		boolean isCheckPrimaryKey = true;

		// 如果主键为0，则设置主键
		Map<Number, Object> map = new HashMap<Number, Object>(entitySize);
		Number pk = null, maxPk = 0;
		Object entity = null;
		for (int i = 0; i < entitySize; i++) {
			entity = entities.get(i);
			pk = ReflectUtil.getPkValue(entity);
			if (pk == null || pk.longValue() == 0) {
				map.put(i, entity);
			} else {
				pks[i] = pk;
				maxPk = pk.intValue() > maxPk.intValue() ? pk : maxPk;
			}
		}
		if (!map.isEmpty()) {
			isCheckPrimaryKey = false;
			int[] newPks = this.idGenerator.genClusterUniqueIntIdBatch(Const.ZK_PRIMARYKEY + "/" + db.getClusterName(),
					tableName, map.size(), maxPk.longValue());
			int i = 0;
			for (Map.Entry<Number, Object> entry : map.entrySet()) {
				int pos = entry.getKey().intValue();
				try {
					ReflectUtil.setPkValue(entities.get(pos), newPks[i]);
				} catch (Exception e) {
					throw new DBOperationException(e);
				}
				pks[pos] = newPks[i];
				i++;
			}
		}

		if (isCheckPrimaryKey)
			this.idGenerator.checkAndSetPrimaryKey(maxPk.longValue(), db.getClusterName(), tableName);

		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_saveBatch(conn, entities, db.getTableIndex());

			if (isCacheAvailable(clazz)) {
				primaryCache.incrCount(db, pks.length);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (Exception e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

		return pks;
	}

	@Override
	public void update(Object entity, IShardingKey<?> shardingKey) {
		List<Object> entities = new ArrayList<Object>();
		entities.add(entity);
		updateBatch(entities, shardingKey);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
		Class<?> clazz = entities.get(0).getClass();

		String talbeName = ReflectUtil.getTableName(clazz);
		ShardingDBResource db = _getDbFromMaster(talbeName, shardingKey);
		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_updateBatch(conn, entities, db.getTableIndex());

			// 清理缓存
			if (isCacheAvailable(clazz)) {
				List pks = new ArrayList(entities.size());
				for (Object entity : entities) {
					pks.add((Number) ReflectUtil.getPkValue(entity));
				}
				primaryCache.remove(db, pks);
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

	}

	@Override
	public void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz) {
		List<Number> pks = new ArrayList<Number>(1);
		pks.add(pk);
		removeByPks(pks, shardingKey, clazz);
	}

	@Override
	public void removeByPks(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<?> clazz) {
		String talbeName = ReflectUtil.getTableName(clazz);
		ShardingDBResource db = _getDbFromMaster(talbeName, shardingKey);

		Connection conn = null;
		try {
			conn = db.getDatasource().getConnection();

			_removeByPks(conn, pks, clazz, db.getTableIndex());

			// 删除缓存
			if (isCacheAvailable(clazz)) {
				primaryCache.remove(db, pks);
				primaryCache.decrCount(db, pks.size());
			}
			if (isSecondCacheAvailable(clazz)) {
				secondCache.remove(db);
			}
		} catch (SQLException e) {
			throw new DBOperationException(e);
		} finally {
			SQLBuilder.close(conn);
		}

	}

	/**
	 * 路由选择.
	 * 
	 * @param clazz
	 *            数据对象
	 * @param shardingKey
	 *            路由因子
	 */
	private ShardingDBResource _getDbFromMaster(String tableName, IShardingKey<?> shardingKey) {
		ShardingDBResource shardingDBResource = null;
		try {
			shardingDBResource = (ShardingDBResource) this.dbCluster.selectDBResourceFromMaster(tableName, shardingKey);
			if (LOG.isDebugEnabled()) {
				LOG.debug("[" + shardingDBResource + "]");
			}
		} catch (DBClusterException e) {
			throw new DBOperationException(e);
		}
		return shardingDBResource;
	}

}

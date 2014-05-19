package com.pinus.datalayer.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.datalayer.IShardingStatistics;
import com.pinus.datalayer.beans.DBClusterStatInfo;
import com.pinus.util.ReflectUtil;

/**
 * 统计接口实现.
 * 
 * @author duanbn
 * 
 */
public class ShardingStatisticsImpl extends AbstractShardingQuery implements IShardingStatistics {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(ShardingStatisticsImpl.class);

	/**
	 * 集群
	 */
	private IDBCluster dbCluster;

	@Override
	public DBClusterStatInfo statEntity(String clusterName, Class<?> clazz) {
		String tableName = ReflectUtil.getTableName(clazz);

		Map<DB, Integer> shardingEntityCount = new HashMap<DB, Integer>();

		List<DataSource> dsList = this.dbCluster.getMasterDsCluster().get(clusterName);
		Map<Integer, Map<String, Integer>> oneCluster = this.dbCluster.getTableCluster().get(clusterName);
		if (oneCluster == null || dsList == null) {
			LOG.warn("找不到集群信息, clusterName=" + clusterName);
			return null;
		}
		DB db = null;
		for (Map.Entry<Integer, Map<String, Integer>> entry : oneCluster.entrySet()) {

			int dbIndex = entry.getKey();
			Map<String, Integer> oneTableCluster = entry.getValue();

			for (Map.Entry<String, Integer> entry1 : oneTableCluster.entrySet()) {
				if (entry1.getKey().equals(tableName)) {
					DataSource ds = dsList.get(dbIndex);
					// 遍历所有的表
					for (int i = 0; i < entry1.getValue(); i++) {
						db = new DB();
						try {
							db.setDbConn(ds.getConnection());
						} catch (SQLException e) {
							LOG.warn(e);
						}
						db.setClusterName(clusterName);
						db.setDbIndex(dbIndex);
						db.setTableName(tableName);
						db.setTableIndex(i);

						int count = selectCountWithCache(db, clazz).intValue();
						shardingEntityCount.put(db, count);
						try {
							db.getDbConn().close();
						} catch (SQLException e) {
							LOG.warn(e);
						}
					}
				}
			}
		}

		DBClusterStatInfo statInfo = new DBClusterStatInfo();
		statInfo.setShardingEntityCount(shardingEntityCount);
		return statInfo;
	}

	public IDBCluster getDbCluster() {
		return dbCluster;
	}

	public void setDbCluster(IDBCluster dbCluster) {
		this.dbCluster = dbCluster;
	}

}

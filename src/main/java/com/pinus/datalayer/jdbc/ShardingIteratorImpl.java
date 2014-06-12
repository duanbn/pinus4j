package com.pinus.datalayer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.pinus.api.query.Condition;
import com.pinus.api.query.IQuery;
import com.pinus.api.query.Order;
import com.pinus.cluster.beans.DBClusterInfo;
import com.pinus.cluster.beans.DBClusterRegionInfo;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.datalayer.IShardingIterator;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.datalayer.beans.DBClusterIteratorInfo;
import com.pinus.util.ReflectUtil;

/**
 * 遍历集群数据接口实现.
 * 
 * @author duanbn
 * @since 0.6.0
 * 
 * @param <E>
 *            数据类型
 */
public class ShardingIteratorImpl<E> implements IShardingIterator<E> {

	/**
	 * 数据库集群引用.
	 */
	private DBClusterInfo dbClusterInfo;

	private Class<E> clazz;
    private int stepLength;

	/**
	 * 查询条件.
	 */
	private IQuery query;

	/**
	 * 当前遍历页号.
	 */
	private int curPage;

	private int maxRegionIndex;
	private int maxDbIndex;
	private int maxTableIndex;
	private long maxId;

	/**
	 * 最有一次遍历的region
	 */
	private int latestRegionIndex;

	/**
	 * 最后一次遍历的数据库下表
	 */
	private int latestDbIndex;

	/**
	 * 最后一次遍历的表下标
	 */
	private int latestTableIndex;

	/**
	 * 最后一次遍历的数据主键
	 */
	private long latestId;

	private final Queue<Object> dataQ = new LinkedList<Object>();

	public ShardingIteratorImpl(Class<E> clazz) {
		this.clazz = clazz;
	}

	public void init() {
		// init max region index
		this.maxRegionIndex = dbClusterInfo.getDbRegions().size() - 1;

        int dbNum = dbClusterInfo.getDbRegions().get(0).getMasterConnection().size();
		// init max db index
		this.maxDbIndex = dbNum - 1;

        int tableNum = maxTableIndex + 1;
        // compute stepLength
        this.stepLength = dbNum * tableNum / 2 * 1000;
        // compute cur page
        this.curPage = (int) latestId / stepLength + 1;
        
		// init latest region index
		if (latestId > 0) {
			DBClusterRegionInfo r = null;
			for (int i = 0; i <= maxRegionIndex; i++) {
				r = dbClusterInfo.getDbRegions().get(i);
				if (latestId >= r.getStart() && latestId <= r.getEnd()) {
					latestRegionIndex = i;
					break;
				}
			}
		}
		
		// init maxId
		DBConnectionInfo connInfo = _getConnectionInfo(latestRegionIndex, latestDbIndex);
		try {
			maxId = _getMaxId(connInfo);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean hasNext() {
		if (dataQ.isEmpty()) {
			try {
				_fill();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		return !dataQ.isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Override
	public E next() {
        E entity = (E) dataQ.poll();
        latestId = ReflectUtil.getPkValue(entity).longValue();
		return entity;
	}

	@Override
	public int curDbIndex() {
		return this.latestDbIndex;
	}

	@Override
	public int curTableIndex() {
		return this.latestTableIndex;
	}

    @Override
    public long curEntityId() {
    	return latestId;
    }

    @Override
    public DBClusterIteratorInfo curIteratorInfo() {
        DBClusterIteratorInfo info = new DBClusterIteratorInfo(this.latestDbIndex, this.latestTableIndex, this.latestId);
        return info;
    }

	/**
	 * get max id
	 */
	private long _getMaxId(DBConnectionInfo connInfo) throws SQLException {
		IQuery maxIdQuery = this.query.clone();
		maxIdQuery.orderBy("id", Order.DESC).limit(1);
		String sql = SQLBuilder.buildSelectByQuery(clazz, latestTableIndex, maxIdQuery);

		long maxId = 0;

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			conn = connInfo.getDatasource().getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				maxId = rs.getLong("id");
			}
		} finally {
			SQLBuilder.close(conn, ps, rs);
		}

		return maxId;
	}

	private DBConnectionInfo _getConnectionInfo(int regionIndex, int dbIndex) {
		DBClusterRegionInfo region = dbClusterInfo.getDbRegions().get(regionIndex);

		List<DBConnectionInfo> regionConnInfos = region.getMasterConnection();
		DBConnectionInfo connInfo = regionConnInfos.get(dbIndex);

		return connInfo;
	}

	private List<E> getData(DBConnectionInfo connInfo, long latestId) throws SQLException {
		IQuery itQuery = this.query.clone();
		itQuery.add(Condition.gt("id", latestId)).add(Condition.lte("id", latestId + stepLength));
		String sql = SQLBuilder.buildSelectByQuery(clazz, latestTableIndex, itQuery);

		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			conn = connInfo.getDatasource().getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			List<E> list = SQLBuilder.buildResultObject(clazz, rs);

			return list;
		} finally {
			SQLBuilder.close(conn, ps, rs);
		}
	}

	private void _fill() throws SQLException {
        //System.out.println("regionIndex=" + latestRegionIndex + ", dbIndex=" + latestDbIndex + ", tableIndex=" + latestTableIndex);

		if (latestRegionIndex <= maxRegionIndex) {
			if (latestDbIndex <= maxDbIndex) {
				DBConnectionInfo connInfo = _getConnectionInfo(latestRegionIndex, latestDbIndex);
				if (latestTableIndex <= maxTableIndex) {
					if (latestId < maxId) {
						dataQ.addAll(getData(connInfo, latestId));
						latestId = curPage++ * stepLength;
					} else {
						latestId = 0;
						curPage = 0;
						if (++latestTableIndex <= maxTableIndex) {
							maxId = _getMaxId(connInfo);
						}
					}
				} else {
					latestTableIndex = 0;
					latestId = 0;
					curPage = 0;
					if (++latestDbIndex <= maxDbIndex) {
						maxId = _getMaxId(connInfo);
					}
				}
			} else {
				latestDbIndex = 0;
				latestTableIndex = 0;
				latestId = 0;
				curPage = 0;
				if (++latestRegionIndex <= maxRegionIndex) {
					DBConnectionInfo connInfo = _getConnectionInfo(latestRegionIndex, latestDbIndex);
					maxId = _getMaxId(connInfo);
				}
			}

			// 进行递归遍历
			if (dataQ.isEmpty()) {
				_fill();
			}
		}
	}

	public DBClusterInfo getDbClusterInfo() {
		return dbClusterInfo;
	}

	public void setDbClusterInfo(DBClusterInfo dbClusterInfo) {
		this.dbClusterInfo = dbClusterInfo;
	}

	public IQuery getQuery() {
		return query;
	}

	@Override
	public void setQuery(IQuery query) {
		this.query = query;
	}

	public int getLatestDbIndex() {
		return latestDbIndex;
	}

	public void setLatestDbIndex(int latestDbIndex) {
		this.latestDbIndex = latestDbIndex;
	}

	public int getLatestTableIndex() {
		return latestTableIndex;
	}

	public void setLatestTableIndex(int latestTableIndex) {
		this.latestTableIndex = latestTableIndex;
	}

	public int getMaxTableIndex() {
		return maxTableIndex;
	}

	public void setMaxTableIndex(int maxTableIndex) {
		this.maxTableIndex = maxTableIndex;
	}

	public long getLatestId() {
		return latestId;
	}

	public void setLatestId(long latestId) {
		this.latestId = latestId;
	}
}

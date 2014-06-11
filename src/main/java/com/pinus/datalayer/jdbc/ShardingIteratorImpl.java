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
import com.pinus.constant.Const;
import com.pinus.datalayer.IShardingIterator;
import com.pinus.datalayer.SQLBuilder;

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

	/**
	 * 查询条件.
	 */
	private IQuery query;

	/**
	 * 当前遍历页号.
	 */
	private int curPage;
    /**
     * 单表总页数
     */
    private int totalPage;

    private int maxRegionIndex;
    private int maxDbIndex;
    private int maxTableIndex;

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

	private Queue<Object> dataQ = new LinkedList<Object>();

	public ShardingIteratorImpl(Class<E> clazz) {
		this.clazz = clazz;
	}

	public void init() {
		// init max region index
		this.maxRegionIndex = dbClusterInfo.getDbRegions().size() - 1;
		// init max db index
		this.maxDbIndex = dbClusterInfo.getDbRegions().get(0).getMasterConnection().size() - 1;

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
            totalPage = _getTotalPage(connInfo);
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
		return (E) dataQ.poll();
	}

	@Override
	public int curDbIndex() {
		return this.latestDbIndex;
	}

	@Override
	public int curTableIndex() {
		return this.latestTableIndex;
	}

	/**
	 * get max id
	 */
	private int _getTotalPage(DBConnectionInfo connInfo) throws SQLException {
		IQuery totalPageQuery = this.query.clone();
		String sql = SQLBuilder.buildSelectCountByQuery(clazz, latestTableIndex, totalPageQuery);

        long count = 0;

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			conn = connInfo.getDatasource().getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				count = rs.getLong(1);
			}
		} finally {
			SQLBuilder.close(conn, ps, rs);
		}

        int totalPage = 0;
        if (count % Const.ITERATOR_DEFAULT_BUFFER == 0) {
            totalPage = (int) (count / Const.ITERATOR_DEFAULT_BUFFER);
        } else {
            totalPage = (int) (count / Const.ITERATOR_DEFAULT_BUFFER + 1);
        }

		return totalPage;
	}

	private DBConnectionInfo _getConnectionInfo(int regionIndex, int dbIndex) {
		DBClusterRegionInfo region = dbClusterInfo.getDbRegions().get(regionIndex);

		List<DBConnectionInfo> regionConnInfos = region.getMasterConnection();
		DBConnectionInfo connInfo = regionConnInfos.get(dbIndex);

		return connInfo;
	}

	private List<E> getPageData(DBConnectionInfo connInfo) throws SQLException {
		IQuery itQuery = this.query.clone();
        itQuery.limit(curPage++ * Const.ITERATOR_DEFAULT_BUFFER, Const.ITERATOR_DEFAULT_BUFFER);
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
		if (latestRegionIndex <= maxRegionIndex) {
			if (latestDbIndex <= maxDbIndex) {
                DBConnectionInfo connInfo = _getConnectionInfo(latestRegionIndex, latestDbIndex);
				if (latestTableIndex <= maxTableIndex) {
					if (curPage < totalPage) {
						dataQ.addAll(getPageData(connInfo));
					} else {
						latestId = 0;
						curPage = 0;
						if (++latestTableIndex <= maxTableIndex) {
                            totalPage = _getTotalPage(connInfo);
                        }
					}
				} else {
					latestTableIndex = 0;
					latestId = 0;
					curPage = 0;
					if (++latestDbIndex <= maxDbIndex) {
                        totalPage = _getTotalPage(connInfo);
                    }
				}
			} else {
				latestDbIndex = 0;
				latestTableIndex = 0;
				latestId = 0;
				curPage = 0;
				if (++latestRegionIndex <= maxRegionIndex) {
                    DBConnectionInfo connInfo = _getConnectionInfo(latestRegionIndex, latestDbIndex);
                    totalPage = _getTotalPage(connInfo);
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

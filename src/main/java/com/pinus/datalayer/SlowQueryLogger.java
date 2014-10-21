package com.pinus.datalayer;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.pinus.api.SQL;
import com.pinus.cluster.DB;

/**
 * 记录数据库的慢查询日志.
 * 
 * @author duanbn
 * 
 */
public class SlowQueryLogger {

	/**
	 * 日志
	 */
	public static final Logger LOG = Logger.getLogger(SlowQueryLogger.class);

	public static void write(DB db, SQL sql, long constTime) {
		LOG.warn("[" + db + "] \"" + sql.toString() + "\" const " + constTime + "ms");
	}

	public static void write(DB db, String sql, long constTime) {
		LOG.warn("[" + db + "] \"" + sql + "\" const " + constTime + "ms");
	}

	public static void write(Connection conn, SQL sql, long constTime) {
		String url = null;
		String dbName = null;
		try {
			url = conn.getMetaData().getURL().substring(13);
			dbName = conn.getCatalog();
		} catch (SQLException e) {
		}
		String host = url.substring(0, url.indexOf("/"));
		LOG.warn(host + " " + dbName + " " + " \"" + sql.toString() + "\"" + constTime + "ms");
	}

	public static void write(Connection conn, String sql, long constTime) {
		String url = null;
		String dbName = null;
		try {
			url = conn.getMetaData().getURL().substring(13);
			dbName = conn.getCatalog();
		} catch (SQLException e) {
		}
		String host = url.substring(0, url.indexOf("/"));
		LOG.warn(host + " " + dbName + " " + " \"" + sql + "\" " + constTime + "ms");
	}

}

package org.pinus4j.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * util for jdbc.
 * 
 * @author duanbn
 *
 */
public class JdbcUtil {

	/**
	 * close jdbc connection
	 * @param conn
	 */
    public static void close(Connection conn) {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            throw new IllegalStateException (e);
        } 
    }

	/**
	 * 关闭数据库相关资源.
	 * 
	 * @param conn
	 *            数据库连接
	 * @param ps
	 *            PreparedStatement对象
	 */
	public static void close(PreparedStatement ps) {
		close(ps, null);
	}

	/**
	 * 关闭数据库Statement.
	 */
	public static void close(Statement st) {
        try {
            if (st != null)
                st.close();
        } catch (SQLException e) {
            throw new IllegalStateException (e);
        }
	}

	/**
	 * 关闭数据库相关资源.
	 * 
	 * @param conn
	 *            数据库连接
	 * @param ps
	 *            PreparedStatement对象
	 * @param rs
	 *            查询结果集
	 */
	public static void close(PreparedStatement ps, ResultSet rs) {
		try {
			if (ps != null) {
				ps.close();
			}
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
            throw new IllegalStateException(e);
		}
	}
	
}

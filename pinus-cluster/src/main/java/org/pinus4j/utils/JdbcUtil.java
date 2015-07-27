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

package com.pinus.generator.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pinus.cluster.beans.DBTable;
import com.pinus.datalayer.SQLBuilder;
import com.pinus.exception.DDLException;
import com.pinus.generator.AbstractDBGenerator;

/**
 * MYSQL数据库生成器的实现. 用于生成MYSQL相关的数据表.
 * 
 * @author duanbn
 */
public class DBMySqlGeneratorImpl extends AbstractDBGenerator {

	/**
	 * 日志.
	 */
	public static final Logger LOG = Logger.getLogger(DBMySqlGeneratorImpl.class);

	public static final String SQL_SHOWTABLE = "show tables";

	private static final Map<String, List<String>> existsTable = new HashMap<String, List<String>>();

	@Override
	public List<String> getTable(Connection conn) throws DDLException {

		String url = null;
		try {
			url = conn.getMetaData().getURL();
		} catch (SQLException e1) {
			throw new DDLException(e1);
		}
		if (existsTable.containsKey(url)) {
			return existsTable.get(url);
		}

		// 保存已经在库中存在的表名
		List<String> tables = new ArrayList<String>();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(SQL_SHOWTABLE);
			rs = ps.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString(1);
				tables.add(tableName);
			}

			existsTable.put(url, tables);
		} catch (SQLException e) {
			throw new DDLException(e);
		} finally {
			SQLBuilder.close(null, ps, rs);
		}

		return tables;
	}

	@Override
	public void syncTable(Connection conn, DBTable table) throws DDLException {

		List<String> tables = getTable(conn);
		if (tables.contains(table.getNameWithIndex())) {
			return;
		}

		try {
			Statement s = null;
			for (String sql : table.getCreateSQL()) {
				try {
					s = conn.createStatement();
					LOG.info(sql);
					s.execute(sql);
				} finally {
					if (s != null) {
						s.close();
					}
				}
			}
		} catch (Exception e) {
			String ignore = "Table '" + table.getNameWithIndex() + "' already exists";
			if (!e.getMessage().equals(ignore))
				throw new DDLException("create table =" + table.getName() + " failure", e);
		}
	}

	@Override
	public void syncTable(Connection conn, DBTable table, int num) throws DDLException {
		if (num <= 0) {
			LOG.warn("生成表的数量为0, 忽略生成数据表, 请检查零库的shard_cluster表的配置");
			return;
		}

		for (int i = 0; i < num; i++) {
			table.setTableIndex(i);
			syncTable(conn, table);
		}
	}

}

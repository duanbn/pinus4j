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

package org.pinus.contrib.commandline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.sql.DataSource;

import jline.Completor;
import jline.ConsoleReader;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import org.pinus.api.IShardingKey;
import org.pinus.api.ShardingKey;
import org.pinus.api.enums.EnumDB;
import org.pinus.cluster.DB;
import org.pinus.cluster.IDBCluster;
import org.pinus.cluster.beans.DBInfo;
import org.pinus.cluster.impl.AppDBClusterImpl;
import org.pinus.exception.DBClusterException;
import org.pinus.generator.beans.DBTable;

/**
 * pinus command line application.
 * 
 * @author duanbn.
 */
public class App {

	/**
	 * prompt of command line.
	 */
	private final String CMD_PROMPT = "pinus-cli>";
	private final String KEY_SHARDINGBY = "sharding by";

	/**
	 * db cluster info.
	 */
	private IDBCluster dbCluster;

	/**
	 * cluster table info.
	 */
	private List<DBTable> tables;

	private DBTable _getDBTableByName(String tableName) throws CommandException {
		// find sharding info
		DBTable dbTable = null;
		for (DBTable one : this.tables) {
			if (one.getName().equals(tableName)) {
				dbTable = one;
				break;
			}
		}
		if (dbTable == null) {
			throw new CommandException("cann't find cluster info about table \"" + tableName + "\"");
		}
		return dbTable;
	}

	private DBTable _getDBTableBySql(String sql) throws CommandException {
		try {
			// parse table name from sql.
			Statement st = CCJSqlParserUtil.parse(sql);
			Select selectStatement = (Select) st;
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			List<String> tableNames = tablesNamesFinder.getTableList(selectStatement);
			if (tableNames.size() != 1) {
				throw new CommandException("have not support multiple table operation");
			}
			String tableName = tableNames.get(0);

			return _getDBTableByName(tableName);
		} catch (Exception e) {
			throw new CommandException("syntax error: " + sql);
		}

	}

	private SqlNode parseGlobalSqlNode(String cmd) throws CommandException {
		try {
			String sql = cmd;

			DBTable dbTable = _getDBTableBySql(sql);
			if (dbTable.getShardingNum() > 0) {
				throw new CommandException(dbTable.getName() + " is not a global table");
			}

			//
			// create sharding key
			//
			String cluster = dbTable.getCluster();

			DBInfo connInfo = this.dbCluster.getMasterGlobalConn(cluster);

			SqlNode sqlNode = new SqlNode();
			sqlNode.setDs(connInfo.getDatasource());
			sqlNode.setSql(sql);

			return sqlNode;
		} catch (DBClusterException e) {
			throw new CommandException(e.getMessage());
		}
	}

	private SqlNode parseShardingSqlNode(String cmd) throws CommandException {

		try {
			String sql = cmd.substring(0, cmd.indexOf(KEY_SHARDINGBY) - 1).trim();
			String shardingValue = cmd.substring(cmd.indexOf(KEY_SHARDINGBY) + 11).trim();

			DBTable dbTable = _getDBTableBySql(sql);
			if (dbTable.getShardingNum() == 0) {
				throw new CommandException(dbTable.getName() + " is not a sharding table");
			}

			String tableName = dbTable.getName();
			//
			// create sharding key
			//
			String cluster = dbTable.getCluster();
			
			// handle String and Number
			IShardingKey<?> key = null;
			if (shardingValue.startsWith("\"") && shardingValue.endsWith("\"")) {
				key = new ShardingKey<String>(cluster, shardingValue.substring(1, shardingValue.length() - 1));
			} else {
				key = new ShardingKey<Long>(cluster, Long.parseLong(shardingValue));
			}

			DB db = null;
			try {
				db = this.dbCluster.selectDbFromMaster(tableName, key);
				System.out.println(db);
			} catch (DBClusterException e) {
				throw new RuntimeException(e);
			}
			sql = sql.replaceAll(tableName, db.getTableName() + db.getTableIndex());

			SqlNode sqlNode = new SqlNode();
			sqlNode.setDs(db.getDatasource());
			sqlNode.setSql(sql);

			return sqlNode;
		} catch (IndexOutOfBoundsException e) {
			throw new CommandException("syntax error: " + cmd);
		}
	}

	private void _handleTotal(String cmd) throws CommandException {
		String tableName = cmd.substring("total ".length() - 1).trim();
		DBTable dbTable = _getDBTableByName(tableName);

		int tableNum = dbTable.getShardingNum();
		String clusterName = dbTable.getCluster();
		List<DB> dbs = this.dbCluster.getAllMasterShardingDB(tableNum, clusterName, tableName);

		long totalCount = 0;
		for (DB db : dbs) {
			DataSource ds = db.getDatasource();
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				conn = ds.getConnection();
				ps = conn.prepareStatement("select count(*) from " + db.getTableName() + db.getTableIndex());
				rs = ps.executeQuery();
				if (rs.next()) {
					totalCount += rs.getLong(1);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					if (rs != null) {
						rs.close();
					}
					if (ps != null) {
						ps.close();
					}
					if (conn != null) {
						conn.close();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("total count " + totalCount);
	}

	/**
	 * handle select sql.
	 * 
	 * @throws CommandException
	 */
	private void _handleSelect(String cmd) throws SQLException, CommandException {
		SqlNode sqlNode = null;
		if (cmd.indexOf(KEY_SHARDINGBY) > -1) {
			sqlNode = parseShardingSqlNode(cmd);
		} else {
			sqlNode = parseGlobalSqlNode(cmd);
		}

		String sql = sqlNode.getSql();

		DataSource ds = sqlNode.getDs();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = ds.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			// show table header
			StringBuilder format = new StringBuilder();
			List<Object> info = new ArrayList<Object>();
			for (int i = 1; i <= columnCount; i++) {
				format.append("%-" + 32 + "s").append("  ");
				info.add(rsmd.getColumnName(i));
			}
			format.append("\n");
			System.out.printf(format.toString(), info.toArray(new Object[info.size()]));
			// show table record
			while (rs.next()) {
				info.clear();
				for (int i = 1; i <= columnCount; i++) {
					info.add(rs.getObject(i));
				}
				System.out.printf(format.toString(), info.toArray(new Object[info.size()]));
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	/**
	 * handle update sql.
	 */
	private void _handleUpdate(String cmd) {
		System.out.println("暂时不支持");
	}

	/**
	 * handle delete sql.
	 */
	private void _handleDelete(String cmd) {
		System.out.println("暂时不支持");
	}

	/**
	 * handle show command.
	 */
	private void _handleShow() {
		StringBuilder info = new StringBuilder();
		boolean isSharding = false;

		System.out.printf("%-30s  |  %-8s  |  %-10s  |  %-30s  |  %-5s\n", "name", "type", "cluster", "sharding field",
				"sharding number");

		for (DBTable table : tables) {

			if (table.getShardingNum() > 0) {
				isSharding = true;
			} else {
				isSharding = false;
			}

			String type = "";
			info.append("type:");
			if (isSharding) {
				type = "sharding";
			} else {
				type = "global";
			}

			String shardingField = "";
			if (isSharding)
				shardingField = table.getShardingBy();

			info.setLength(0);

			System.out.printf("%-30s  |  %-8s  |  %-10s  |  %-30s  |  %-5d\n", table.getName(), type,
					table.getCluster(), shardingField, table.getShardingNum());
		}
	}

	public void _handleHelp() {
		StringBuilder helpInfo = new StringBuilder();
		helpInfo.append("show       - 显示数据表及每个表的分片信息\n");
		helpInfo.append("total      - 显示总数\n");
		helpInfo.append("select语法 - 分片表 stand sql + sharding by ${value}; e.g: select * from tablename where field=value sharding by 50|\"50\"\n");
		helpInfo.append("             全局表 stand sql\n");
		helpInfo.append("help       - 显示帮助信息\n");
		helpInfo.append("exit       - 退出");
		System.out.println(helpInfo.toString());
	}

	public App(String storageConfigFile) throws Exception {
		dbCluster = new AppDBClusterImpl(EnumDB.MYSQL);
		dbCluster.setShardInfoFromZk(true);
		dbCluster.startup(storageConfigFile);

		this.tables = dbCluster.getDBTableFromZk();

		// sort by cluster name
		Collections.sort(this.tables, new Comparator<DBTable>() {
			@Override
			public int compare(DBTable o1, DBTable o2) {
				return o1.getCluster().compareTo(o2.getCluster());
			}
		});
	}

	public void run() throws Exception {
		boolean isRunning = true;

		ConsoleReader creader = new ConsoleReader();
		creader.addCompletor(new JlineCompletor());
		String cmd = null;
		while (isRunning) {
			try {
				cmd = creader.readLine(CMD_PROMPT);
				if (cmd.endsWith(";")) {
					cmd = cmd.substring(0, cmd.length() - 1);
				}

				if (cmd.equals("exit")) {
					isRunning = false;
				} else if (cmd.toLowerCase().startsWith("select")) {
					_handleSelect(cmd);
				} else if (cmd.toLowerCase().startsWith("update")) {
					_handleUpdate(cmd);
				} else if (cmd.toLowerCase().startsWith("delete")) {
					_handleDelete(cmd);
				} else if (cmd.toLowerCase().startsWith("total")) {
					_handleTotal(cmd);
				} else if (cmd.toLowerCase().equals("show")) {
					_handleShow();
				} else if (cmd.trim().equals("help")) {
					_handleHelp();
				} else if (cmd.trim().equals("")) {
				} else {
					System.out.println("unknow command:\"" + cmd + "\", now support select, update, delete ");
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

		this.dbCluster.shutdown();
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("usage: pinus-cli.sh [storage-config.xml path]");
			System.exit(-1);
		}
		String storageConfigFile = args[0];

		App app = new App(storageConfigFile);
		app.run();

		System.out.println("see you :)");
	}

	private class CommandException extends Exception {

		private static final long serialVersionUID = 1L;

		public CommandException(String msg) {
			super(msg);
		}

	}

	private class JlineCompletor implements Completor {

		@Override
		public int complete(String buffer, int cursor, List candidates) {
			if (buffer.indexOf("from") > -1) {
				String prefix = buffer.substring(buffer.indexOf("from") + 4).trim();
				if (prefix.equals("")) {
					for (DBTable table : tables) {
						candidates.add(table.getName());
					}
				} else {
					for (DBTable table : tables) {
						if (table.getName().startsWith(prefix)) {
							candidates.add(table.getName());
						}
					}
				}

				cursor -= prefix.length();
			}

			return cursor;
		}

	}

	private class SqlNode {
		private DataSource ds;
		private String sql;

		public DataSource getDs() {
			return ds;
		}

		public void setDs(DataSource ds) {
			this.ds = ds;
		}

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}
	}

}

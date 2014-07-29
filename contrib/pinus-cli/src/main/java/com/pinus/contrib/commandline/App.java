package com.pinus.contrib.commandline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import jline.ConsoleReader;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import com.pinus.api.IShardingKey;
import com.pinus.api.ShardingKey;
import com.pinus.api.enums.EnumDB;
import com.pinus.cluster.DB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.beans.DBConnectionInfo;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.impl.DbcpDBClusterImpl;
import com.pinus.exception.DBClusterException;

/**
 * pinus command line application.
 * 
 * @author duanbn.
 */
public class App {

	/**
	 * prompt of command line.
	 */
	public final String CMD_PROMPT = "pinus-cli>";
	public final String KEY_SHARDINGBY = "sharding by";

	/**
	 * db cluster info.
	 */
	private IDBCluster dbCluster;

	/**
	 * cluster table info.
	 */
	private List<DBTable> tables;

    private DBTable _getDBTableBySql(String sql) throws JSQLParserException {
        // parse table name from sql.
        Statement st = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) st;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableNames = tablesNamesFinder.getTableList(selectStatement);
        if (tableNames.size() != 1) {
            throw new CommandException("have not support multiple table operation");
        }
        String tableName = tableNames.get(0);

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

            DBConnectionInfo connInfo = this.dbCluster.getMasterGlobalConn(cluster);

			SqlNode sqlNode = new SqlNode();
            sqlNode.setDs(connInfo.getDatasource());
            sqlNode.setSql(sql);

			return sqlNode;
		} catch (JSQLParserException e) {
			throw new CommandException("syntax error: " + cmd);
		} catch (DBClusterException e) {
			throw new CommandException(e.getMessage());
		}
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		} catch (JSQLParserException e) {
			throw new CommandException("syntax error: " + cmd);
		} catch (IndexOutOfBoundsException e) {
			throw new CommandException("syntax error: " + cmd);
		}
	}

	public App(String storageConfigFile) throws Exception {
		dbCluster = new DbcpDBClusterImpl(EnumDB.MYSQL);
		dbCluster.setShardInfoFromZk(true);
		dbCluster.startup(storageConfigFile);

		this.tables = dbCluster.getDBTableFromZk();
	}

	public void run() throws Exception {
		boolean isRunning = true;

		ConsoleReader creader = new ConsoleReader();
		String cmd = null;
		while (isRunning) {
			cmd = creader.readLine(CMD_PROMPT);
            if (cmd.endsWith(";")) {
                cmd = cmd.substring(0, cmd.length() - 1);
            }

			try {
				if (cmd.equals("exit")) {
					isRunning = false;
				} else if (cmd.toLowerCase().startsWith("select")) {
					_handleSelect(cmd);
				} else if (cmd.toLowerCase().startsWith("update")) {
					_handleUpdate(cmd);
				} else if (cmd.toLowerCase().startsWith("delete")) {
					_handleDelete(cmd);
				} else if (cmd.toLowerCase().equals("show")) {
					_handleShow();
				} else if (cmd.trim().equals("")) {
				} else {
					System.out.println("unknow command:\"" + cmd + "\", now support select, update, delete ");
				}
			} catch (CommandException e) {
				System.out.println(e.getMessage());
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	/**
	 * handle select sql.
	 */
	private void _handleSelect(String cmd) throws SQLException {
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
            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                header.append(rsmd.getColumnName(i)).append(" ");
            }
            System.out.println(header.toString());
            // show table record
			StringBuilder record = new StringBuilder();
			while (rs.next()) {
				for (int i = 1; i <= columnCount; i++) {
					record.append(rs.getObject(i)).append(" | ");
				}
				System.out.println(record.toString());
				record.setLength(0);
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
	}

	/**
	 * handle delete sql.
	 */
	private void _handleDelete(String cmd) {
	}

	/**
	 * handle show command.
	 */
	private void _handleShow() {
		StringBuilder info = new StringBuilder();
		boolean isSharding = false;
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
				shardingField = "sharding field:" + table.getShardingBy();

			info.setLength(0);

			System.out.printf("name:%-30s  |  type:%-8s  |  cluster:%-10s  |  %-30s  |  sharding number:%-5d\n",
					table.getName(), type, table.getCluster(), shardingField, table.getShardingNum());
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("usage: java pinus-cli.jar [storage-config.xml path]");
			System.exit(-1);
		}
		String storageConfigFile = args[0];

		App app = new App(storageConfigFile);
		app.run();

		System.out.println("see you :)");
	}

	private class CommandException extends RuntimeException {

		public CommandException(String msg) {
			super(msg);
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

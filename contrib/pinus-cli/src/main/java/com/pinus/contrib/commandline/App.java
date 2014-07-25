package com.pinus.contrib.commandline;

import java.io.StringReader;
import java.util.List;

import jline.ConsoleReader;

import com.pinus.api.IShardingKey;
import com.pinus.api.enums.EnumDB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.beans.DBTable;
import com.pinus.cluster.impl.DbcpDBClusterImpl;

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

	/**
	 * parse sharding key from command.
	 * 
	 * @param cmd
	 *            command.
	 * 
	 * @return sharding key of pinus.
	 */
	private IShardingKey<?> parseShardingKey(String cmd) throws CommandException {
		try {
			String sql = cmd.substring(0, cmd.indexOf(KEY_SHARDINGBY) - 1).trim();
			String shardingby = cmd.substring(cmd.indexOf(KEY_SHARDINGBY) + 11).trim();
            // TODO : here need parse sql, then get table name for get cluster info.
		} catch (Exception e) {
			throw new CommandException("pinus query syntax:" + cmd);
		}

		return null;
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
			// System.out.print(CMD_PROMPT);

			cmd = creader.readLine(CMD_PROMPT);

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
			}
		}
	}

	/**
	 * handle select sql.
	 */
	private void _handleSelect(String cmd) {
		IShardingKey<?> sk = parseShardingKey(cmd);
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
			}

			info.append("name:").append(table.getName()).append(" | ");
			info.append("type:");
			if (isSharding) {
				info.append("sharding");
			} else {
				info.append("global");
			}
			info.append(" | ");
			info.append("cluster:").append(table.getCluster()).append(" | ");
			if (isSharding)
				info.append("sharding field:").append(table.getShardingBy()).append(" | ");
			info.append("sharding number:").append(table.getShardingNum());
			System.out.println(info.toString());
			info.setLength(0);
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

}

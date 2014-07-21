package com.pinus.contrib.commandline;

import jline.ConsoleReader;

import com.pinus.api.enums.EnumDB;
import com.pinus.cluster.IDBCluster;
import com.pinus.cluster.impl.DbcpDBClusterImpl;

public class App {

	public final String CMD_PROMPT = "pinus-cli>";

    public void run(String storageConfigFile) throws Exception {
        IDBCluster dbCluster = new DbcpDBClusterImpl(EnumDB.MYSQL);
		dbCluster.setShardInfoFromZk(true);
		dbCluster.startup(storageConfigFile);

		boolean isRunning = true;

		ConsoleReader creader = new ConsoleReader();
		String cmd = null;
		while (isRunning) {
			//System.out.print(CMD_PROMPT);

            cmd = creader.readLine(CMD_PROMPT);

            if (cmd.equals("exit")) {
                isRunning = false;
            }
            if (cmd.toLowerCase().startsWith("select")) {
                _handleSelect(cmd);
            }

			try {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
    }

    private void _handleSelect(String cmd) {
        System.out.println(cmd);
    }

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("usage: java pinus-cli.jar [storage-config.xml path]");
			System.exit(-1);
		}
		String storageConfigFile = args[0];

        App app = new App();
        app.run(storageConfigFile);

        System.out.println("see you :)");
	}

}

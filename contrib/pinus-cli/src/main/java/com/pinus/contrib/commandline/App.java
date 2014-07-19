package com.pinus.contrib.commandline;

import java.io.*;

import com.pinus.cluster.*;
import com.pinus.cluster.impl.*;
import com.pinus.api.enums.*;

public class App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("usage: java pinus-cli.jar [storage-config.xml path]");
            System.exit(-1);
        }

        String storageConfigFile = args[0];

        IDBCluster dbCluster = new DbcpDBClusterImpl(EnumDB.MYSQL);
        dbCluster.startup();
    }

}

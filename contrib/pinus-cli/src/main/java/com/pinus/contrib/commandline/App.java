package com.pinus.contrib.commandline;

import java.io.*;

public class App {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("usage: java pinus-cli.jar [storage-config.xml path]");
            System.exit(-1);
        }

        String storageConfigFile = args[0];
    }

}

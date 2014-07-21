#!/bin/sh

mvn clean compile
mvn exec:java -Dexec.mainClass="com.pinus.contrib.commandline.App" -Dexec.args="/Users/Asia/workspace/sourcecode/pinus/src/test/resources/storage-config.xml"

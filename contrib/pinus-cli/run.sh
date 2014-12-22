#!/bin/sh

mvn clean package
storageFile="../../src/test/resources/storage-config.xml"
./target/pinus-cli-1.0.0-distribution/pinus-cli-1.0.0/bin/pinus-cli.sh $storageFile

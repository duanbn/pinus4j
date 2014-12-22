#!/bin/sh

basedir=`dirname $0`
basedir=`cd $basedir/..; pwd;`

libdir=$basedir/lib
configdir=$basedir/conf

#load config file
classpath=$classpath:$configdir
classpath=$classpath:$libdir/*

main=org.pinus.contrib.commandline.App

opts="-Xms128m"
exec java $opts -cp $classpath $main $1

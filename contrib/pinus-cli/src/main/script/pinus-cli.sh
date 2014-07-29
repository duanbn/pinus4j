#!/bin/sh

basedir=`dirname $0`
basedir=`cd $basedir/..; pwd;`

libdir=$basedir/lib
configdir=$basedir/conf

classpath=$classpath:$libdir/*
#load config file
classpath=$classpath:$configdir

main=com.pinus.contrib.commandline.App

opts="-Xms128m"
exec java $opts -cp $classpath $main $1

#!/bin/sh

hbase -Xmx2048m -cp build/classes:build/test/classes:\
lib/hbase-0.89.20100726.jar:\
lib/commons-logging-1.1.1.jar:\
lib/log4j-1.2.15.jar:\
lib/hadoop-core-0.20.3-append-r964955-1240.jar:\
lib/zookeeper-3.3.1.jar:\
lib/scala-library.jar:\
lib/commons-cli-1.2.jar:\
/home/hbase/hadoop-lzo/build/hadoop-lzo-0.4.4.jar \
hfileinput.HFileKVRowScannerSpecs $1

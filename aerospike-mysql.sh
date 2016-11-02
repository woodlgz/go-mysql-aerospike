#!/bin/bash

BIN=./bin/
LOG_DIR=`pwd`/logs
mkdir -p $LOG_DIR
cd $BIN
./go-mysql-aerospike 1>$LOG_DIR/go-mysql-aerospike.log 2>&1 &


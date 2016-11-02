#!/bin/bash

BIN=../go-mysql-aerospike/bin/

while [ 1 ];
do
        sleep 10
        gosync=`ps -ef|grep "go-mysql-aerospike"|wc -l`
        if [[ $gosync == 1  ]];
        then
                continue
        fi
        syncerr=`cat logs/go-mysql-as.log |tail -n 10 |grep "Error"|wc -l`
        if [[ $syncerr > 0 ]];
        then
                d=`date`
                echo "$d:go-mysql-aerospike script encounter with error,restart go-mysql-aerospike" >> check.status
                for pid in `ps -ef|grep "go-mysql-aerospike" |awk '{printf("%s\n",$2)}'`;
                do
                        kill $pid
                done
                rm -rf $BIN/var
                ./aerospike-mysql.sh
        fi
done


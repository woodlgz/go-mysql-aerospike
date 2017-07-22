go-mysql-aerospike is a service syncing your MySQL data into aerospike automatically.

It uses `mysqldump` to fetch the origin data at first, then syncs data incrementally with binlog.

This project is inspired by go-mysql-elasticsearch by siddontang and I extend its functionality.

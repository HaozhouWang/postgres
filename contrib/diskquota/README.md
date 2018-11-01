# Overview
Diskquota is an extension that provides disk usage enforcement for database objects in Postgresql. Currently it supports to set quota limit on schema and role in a given database and limit the amount of disk space that a schema or a role can use. 

This project is inspired by Heikki's pg_quota project (link: https://github.com/hlinnaka/pg_quota) and enhance it to support different kinds of DDL and DML which may change the disk usage of database objects. 

Diskquota is a soft limit of disk uages. It has some delay to detect the schemas or roles whose quota limit is exceeded. Here 'soft limit' support two kinds of encforcement:  Query loading data into out-of-quota schema/role will be forbidden before query is running. Query loading data into schema/role with rooms will be cancelled when the quota limit is reached dynamically during the query is running. 

temp table??
# Design
Diskquota extension is based on background worker framework in Postgresql.
There are two kinds of background workers: diskquota launcher and diskquota worker.

There is only one laucher process per database cluster(i.e. one laucher per postmaster).
Launcher process is reponsible for manage worker processes: Calling RegisterDynamicBackgroundWorker() 
to create new workers and keep their handle. Calling TerminateBackgroundWorker() to
terminate workers which are disabled when DBA modify worker_spi.monitor_databases

There are many worker processes, one for each database which is listed in worker_spi.monitor_databases.
Currently, we support to monitor at most 10 databases at the same time.
Worker processes are responsible for monitoring the disk usage of schemas and roles for the target database, 
and do quota enfocement. It will periodically recalcualte the table size of active tables, and update their corresponding schema or owner's disk usage. Then compare with quota limit for those schemas or roles. If exceeds the limit, put the corresponding schemas or roles into the blacklist in shared memory. Schemas or roles in blacklist are used to do query enforcement to cancel queries which plan to load data into these schemas or roles.

Active tables are the tables whose table size may change in the last quota check interval. We use hooks in smgecreate(), smgrextend() and smgrtruncate() to detect active tables and store them(currently relfilenode) in the shared memory. Diskquota worker process will periodically consuming active table in shared memories, convert relfilenode to relaton oid, and calcualte table size by calling pg_total_relation_size(), which will sum the size of table(base, vm, fsm), toast, index.

Enforcement is implemented as hooks. There are two kinds of enforcement hooks: enforcement before query is running and
enforcement during query is running.
The first one is implemented at ExecCheckPerms
The second one is implemented at GetBuffer


# Install
1. Compile and install disk quota.
```
cd contrib/disk_quota; 
make; 
make install
```
2. Config postgres.conf
```
# enable disk_quota in preload library.
shared_preload_libraries = 'disk_quota'
# set monitored databases and naptime to refresh the disk quota stats.
worker_spi.monitor_databases = 'postgres'
worker_spi.naptime = 2
```
3. Create disk_quota extension in monitored database.
```
create extension disk_quota;
```

# Usage
1. Set schema quota limit using diskquota.set_schema_quota
```
create schema s1;
select diskquota.set_schema_quota('s1', '1 MB');
set search_path to s1;

create table a(i int);
# insert small data succeeded
insert into a select generate_series(1,100);
# insert large data failed
insert into a select generate_series(1,10000000);
# insert small data failed
insert into a select generate_series(1,100);
reset search_path;
```

2. Set role quota limit using diskquota.set_role_quota
```
create role u1 nologin;
create table b (i int);
alter table b owner to u1;
select diskquota.set_role_quota('u1', '1 MB');

# insert small data succeeded
insert into b select generate_series(1,100);
# insert large data failed
insert into b select generate_series(1,10000000);
# insert small data failed
insert into b select generate_series(1,100);
```

# Test
Run regression tests.
```
cd contrib/disk_quota; 
make installcheck
```

# Benchmark & Performence Test
## Cost of diskquota worker.
10K user tables
100K user tables
100K user tables with 1K active tables.

## Impact on OLTP queries
We test OLTP queries to measure the impact of enabling diskquota feature.
With diskquota enabled
Without diskquota enabled


#Notes
1. Drop database with diskquota enabled.

If DBA enable monitoring diskquota on a database, there will be a connection
to this database from diskquota worker process. DBA need to first remove this
database from worker_spi.monitor_databases in postgres.conf, and reload 
configuration by call `pg_ctl reload`. Then database could be dropped successfully.

2. Temp table.

Diskquota supports to limit the disk usage of temp table as well. But schema and role are different.
For role, i.e. the owner of the temp table, diakquota will treat it the same as normal tables and sum its
table size to its owner's quota. While for schema, temp table is located under namespace 'pg_temp_backend_id',
so temp table size will not sum to the current schema's qouta.

# Known Issue.
Since Postgresql doesn't support READ UNCOMMITTED isolation level, 
our implementation cannot detect the new created table inside an
uncommitted transaction(See below example). Hence enforcement on 
that newly created table will not work. After transaction commit,
diskquota worker process could detect the newly create table
and do enfocement accordingly in later queries.
```
# suppose quota of schema s1 is 1MB.
set search_path to s1;
create table b;
BEGIN;
create table a;
# Issue: quota enforcement doesn't work on table a
insert into a select generate_series(1,200000);
# quota enforcement works on table b
insert into b select generate_series(1,200000);
# quota enforcement works on table a,
# since quota limit of schema s1 has already exceeds.
insert into a select generate_series(1,200000);
END;
```
One solution direction is that we calculate the additional 'uncommited data size' 
for schema and role in worker process. Since pg_total_relation_size need to hold 
AccessShareLock to relation(And worker process don't even know this reloid exists), 
we need to skip it, and call stat() directly with tolerant to file unlink. 
Skip lock is dangerous and we plan to leave it as known issue at current stage.


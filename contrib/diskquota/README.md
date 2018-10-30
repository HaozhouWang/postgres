# Overview
disk_quota is an extension that provides disk usage enforcement for database objects in Postgresql. It supports to set quota limit on schema and role(currently) in a given database and limit the amount of disk space that a schema or a role can use. 

This project is inspired by Heikki's pg_quota project link(https://github.com/hlinnaka/pg_quota) and enhance it to support different kinds of DDL and DML which may change the disk usage of database objects. 

# Design



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

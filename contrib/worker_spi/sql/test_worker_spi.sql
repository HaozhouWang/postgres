create extension worker_spi;

select pg_sleep(1);

\! pg_ctl -D /tmp/pg_worker_spi_test/data reload
\! cp data/clarge.txt /tmp/clarge.txt
\! cp data/clarge.txt /tmp/csmall.txt
select pg_sleep(5);

-- Test schema quota 
create schema s1;
select diskquota.set_schema_quota_limit('s1', '1 MB');
set search_path to s1;

create table a(i int);
insert into a select generate_series(1,100);
-- expect insert fail
insert into a select generate_series(1,100000000);
-- expect insert fail
insert into a select generate_series(1,100);
create table a2(i int);
-- expect insert fail
insert into a2 select generate_series(1,100);

reset search_path;

-- Test role quota
CREATE role u1 NOLOGIN;
CREATE TABLE b (t text);
ALTER TABLE b OWNER TO u1;

select diskquota.set_role_quota_limit('u1', '1 MB');

insert into b select generate_series(1,100);
-- expect insert fail
insert into b select generate_series(1,100000000);
-- expect insert fail
insert into b select generate_series(1,100);

-- Test alter table set schema
create schema s2;
set search_path to s1;
alter table a set schema s2;
select pg_sleep(3);
-- expect insert succeed
insert into a2 select generate_series(1,20000);
reset search_path;

-- Test alter table add column and truncate
set search_path to s1;
select pg_sleep(3);
insert into a2 select generate_series(1,10);
ALTER TABLE a2 ADD COLUMN j varchar(30);
update a2 set j = 'add value for column j';
select pg_sleep(3);
-- expect insert failed after add column
insert into a2 select generate_series(1,10);
truncate a2;
select pg_sleep(3);
create table a3(i int);
insert into a3 select generate_series(1,30000);
select pg_sleep(3);
-- expect alter table statement failed
ALTER TABLE a3 ADD COLUMN j int default 3;
reset search_path;

-- Test copy
create schema s3;
select diskquota.set_schema_quota_limit('s3', '1 MB');
set search_path to s3;

create table c (i int);
copy c from '/tmp/csmall.txt';
copy c from '/tmp/clarge.txt';
select pg_sleep(3);
-- expect copy fail
copy c from '/tmp/csmall.txt';
reset search_path;

-- Test Update
create schema s4;
select diskquota.set_schema_quota_limit('s4', '1 MB');
set search_path to s4;
create table a(i int);
insert into a select generate_series(1,30000);
select pg_sleep(3);
-- expect update fail.
update a set i = 100;
reset search_path;

-- Test toast
create schema s5;
select diskquota.set_schema_quota_limit('s5', '1 MB');
set search_path to s5;
CREATE TABLE a5 (message text);
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,10000)) 
FROM generate_series(1,10);

-- expect insert toast fail
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,100000)) 
FROM generate_series(1,1000);
reset search_path;

-- Test vacuum full
create schema s6;
select diskquota.set_schema_quota_limit('s6', '1 MB');
set search_path to s6;
create table a6 (i int);
insert into a6 select generate_series(1,30000);
select pg_sleep(3);
-- expect insert fail
insert into a6 select generate_series(1,30);
delete from a6 where i > 100;
vacuum full a6;
select pg_sleep(3);
-- expect insert succeed
insert into a6 select generate_series(1,30);
reset search_path;

-- Test truncate
create schema s7;
select diskquota.set_schema_quota_limit('s7', '1 MB');
set search_path to s7;
create table a7 (i int);
insert into a7 select generate_series(1,30000);
select pg_sleep(3);
-- expect insert fail
insert into a7 select generate_series(1,30);
truncate table a7;
select pg_sleep(3);
-- expect insert succeed
insert into a7 select generate_series(1,30);
reset search_path;

-- Test SPI
drop extension worker_spi;


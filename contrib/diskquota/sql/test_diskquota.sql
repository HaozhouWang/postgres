create extension diskquota;

select pg_sleep(1);

\! pg_ctl -D /home/hawu/db_dir/pg_diskquota_test/data reload
\! cp data/csmall.txt /tmp/csmall.txt
\! df -h
select pg_sleep(5);

-- Test schema quota 
create schema s1;
select diskquota.set_schema_quota('s1', '1 MB');
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

-- Test alter table set schema
create schema s2;
set search_path to s1;
alter table a set schema s2;
select pg_sleep(5);
-- expect insert succeed
insert into a2 select generate_series(1,20000);
-- expect insert succeed
insert into s2.a select generate_series(1,20000);
reset search_path;

-- ##########################################
-- Test role quota
CREATE role u1 NOLOGIN;
CREATE role u2 NOLOGIN;
CREATE TABLE b (t text);
ALTER TABLE b OWNER TO u1;
CREATE TABLE b2 (t text);
ALTER TABLE b2 OWNER TO u1;

select diskquota.set_role_quota('u1', '1 MB');

insert into b select generate_series(1,100);
-- expect insert fail
insert into b select generate_series(1,100000000);
-- expect insert fail
insert into b select generate_series(1,100);
-- expect insert fail
insert into b2 select generate_series(1,100);
alter table b owner to u2;
select pg_sleep(5);
-- expect insert succeed
insert into b select generate_series(1,100);
-- expect insert succeed
insert into b2 select generate_series(1,100);

-- Test alter table add column and truncate
set search_path to s1;
select pg_sleep(5);
insert into a2 select generate_series(1,10);
ALTER TABLE a2 ADD COLUMN j varchar(50);
update a2 set j = 'add value for column j';
select pg_sleep(5);
-- expect insert failed after add column
insert into a2 select generate_series(1,10);
reset search_path;
drop table b, b2;
drop role u1, u2;

-- Test copy
create schema s3;
select diskquota.set_schema_quota('s3', '1 MB');
set search_path to s3;

create table c (i int);
copy c from '/tmp/csmall.txt';
insert into c select generate_series(1,100000000);
select pg_sleep(5);
-- expect copy fail
copy c from '/tmp/csmall.txt';
reset search_path;

-- Test Update
create schema s4;
select diskquota.set_schema_quota('s4', '1 MB');
set search_path to s4;
create table a(i int);
insert into a select generate_series(1,50000);
select pg_sleep(5);
-- expect update fail.
update a set i = 100;
reset search_path;

-- Test toast
create schema s5;
select diskquota.set_schema_quota('s5', '1 MB');
set search_path to s5;
CREATE TABLE a5 (message text);
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,10000)) 
FROM generate_series(1,10);

select pg_sleep(5);
-- expect insert toast fail
INSERT INTO a5
SELECT (SELECT 
        string_agg(chr(floor(random() * 26)::int + 65), '')
        FROM generate_series(1,100000)) 
FROM generate_series(1,1000000);
reset search_path;

-- Test vacuum full
create schema s6;
select diskquota.set_schema_quota('s6', '1 MB');
set search_path to s6;
create table a6 (i int);
insert into a6 select generate_series(1,50000);
select pg_sleep(5);
-- expect insert fail
insert into a6 select generate_series(1,30);
delete from a6 where i > 100;
vacuum full a6;
select pg_sleep(5);
-- expect insert succeed
insert into a6 select generate_series(1,30);
reset search_path;

-- Test truncate
create schema s7;
select diskquota.set_schema_quota('s7', '1 MB');
set search_path to s7;
create table a7 (i int);
insert into a7 select generate_series(1,50000);
select pg_sleep(5);
-- expect insert fail
insert into a7 select generate_series(1,30);
truncate table a7;
select pg_sleep(5);
-- expect insert succeed
insert into a7 select generate_series(1,30);
reset search_path;

-- Test partition table
create schema s8;
select diskquota.set_schema_quota('s8', '1 MB');
set search_path to s8;
CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
)PARTITION BY RANGE (logdate);
CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

CREATE TABLE measurement_y2006m03 PARTITION OF measurement
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');
insert into measurement select generate_series(1,15000), '2006-02-01' ,1,1;
select pg_sleep(5);
insert into measurement select 1, '2006-02-01' ,1,1;
-- expect insert fail
insert into measurement select generate_series(1,100000000), '2006-02-01' ,1,1;
-- expect insert fail
insert into measurement select 1, '2006-02-01' ,1,1;
reset search_path;


-- Test Drop table
create schema sdrtbl;
select diskquota.set_schema_quota('sdrtbl', '1 MB');
set search_path to sdrtbl;
create table a(i int);
insert into a select generate_series(1,100);
-- expect insert fail
insert into a select generate_series(1,100000000);
create table a2(i int);
-- expect insert fail
insert into a2 select generate_series(1,100);
drop table a;
select pg_sleep(5);
-- expect insert succeed
insert into a2 select generate_series(1,100);
reset search_path;

-- Test re-set_schema_quota
create schema srE;
select diskquota.set_schema_quota('srE', '1 MB');
set search_path to srE;
create table a(i int);
-- expect insert fail
insert into a select generate_series(1,100000000);
-- expect insert fail when exceed quota limit
insert into a select generate_series(1,10000);
-- set schema quota larger
select diskquota.set_schema_quota('srE', '1 GB');
select pg_sleep(5);
-- expect insert succeed
insert into a select generate_series(1,10000);
reset search_path;

-- Test temp table restrained by role id
create schema strole;
create role u3 nologin;
set search_path to strole;
select diskquota.set_role_quota('U3', '1MB');
create table a(i int);
alter table a owner to u3;
create temp table ta(i int);
alter table ta owner to u3;
-- expected failed: fill temp table
insert into ta select generate_series(1,100000000);
-- expected failed: 
insert into a select generate_series(1,100);
\! df -h
drop table ta;
insert into a select generate_series(1,100);

reset search_path;

drop extension diskquota;


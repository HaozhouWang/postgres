create extension worker_spi;

select pg_sleep(1);

\! pg_ctl -D /tmp/pg_worker_spi_test/data reload

select pg_sleep(5);

-- Test schema quota 
create schema s1;
select diskquota.set_schema_quota_limit('s1', '1 MB');
set search_path to s1;

create table a(i int);
insert into a select generate_series(1,100);
insert into a select generate_series(1,100000000);
insert into a select generate_series(1,100);

reset search_path;
-- Test role quota
CREATE role u1 NOLOGIN;
CREATE TABLE b (t text);
ALTER TABLE b OWNER TO u1;

select diskquota.set_role_quota_limit('u1', '1 MB');

insert into b select generate_series(1,100);
insert into b select generate_series(1,100000000);
insert into b select generate_series(1,100);


drop extension worker_spi;


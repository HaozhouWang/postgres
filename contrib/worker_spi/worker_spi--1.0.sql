/* contrib/worker_spi/worker_spi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION worker_spi" to load this file. \quit

CREATE SCHEMA diskquota;

set search_path='diskquota';

-- Configuration table
create table diskquota.quota_config (targetOid oid PRIMARY key, quotalimitMB int8);

SELECT pg_catalog.pg_extension_config_dump('diskquota.quota_config', '');

CREATE FUNCTION set_schema_quota_limit(text, text)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION set_role_quota_limit(text, text)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE VIEW show_schema_quota_limit AS
SELECT pg_namespace.nspname as schema_name, pg_class.relnamespace as schema_oid, quota.quotalimitMB as quota_in_mb, sum(pg_total_relation_size(pg_class.oid)) as nspsize_in_bytes
FROM pg_namespace, pg_class, diskquota.quota_config as quota
WHERE pg_class.relnamespace = quota.targetoid and pg_class.relnamespace = pg_namespace.oid
GROUP BY pg_class.relnamespace, pg_namespace.nspname, quota.quotalimitMB;

CREATE VIEW show_role_quota_limit AS
SELECT pg_roles.rolname as role_name, pg_class.relowner as role_oid, quota.quotalimitMB as quota_in_mb, sum(pg_total_relation_size(pg_class.oid)) as rolsize_in_bytes
FROM pg_roles, pg_class, diskquota.quota_config as quota
WHERE pg_class.relowner = quota.targetoid and pg_class.relowner = pg_roles.oid
GROUP BY pg_class.relowner, pg_roles.rolname, quota.quotalimitMB;

CREATE TYPE diskquota_active_table_type AS ("TABLE_OID" oid, "DATABASE_ID" oid, "TABLE_SIZE" int8);

CREATE OR REPLACE FUNCTION diskquota_fetch_active_table_stat() RETURNS setof diskquota_active_table_type
AS 'MODULE_PATHNAME', 'diskquota_fetch_active_table_stat'
LANGUAGE C VOLATILE;

reset search_path;

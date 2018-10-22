/* contrib/worker_spi/worker_spi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION worker_spi" to load this file. \quit

CREATE SCHEMA diskquota;

set search_path='diskquota';

-- Configuration table
create table diskquota.quota_config (targetOid oid PRIMARY key, quotalimitMB int8);

SELECT pg_catalog.pg_extension_config_dump('diskquota.quota_config', '');

CREATE FUNCTION set_schema_quota_limit(cstring,int8)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION set_role_quota_limit(cstring,int8)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

reset search_path;

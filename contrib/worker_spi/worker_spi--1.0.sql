/* contrib/worker_spi/worker_spi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION worker_spi" to load this file. \quit

CREATE FUNCTION worker_spi_launch(pg_catalog.int4)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION set_disk_quota_limit(pg_catalog.int4,pg_catalog.int4)
RETURNS void STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE SCHEMA quota;

set search_path='quota';

-- Configuration table
create table quota.config (targetOid oid PRIMARY key, quota int8);

SELECT pg_catalog.pg_extension_config_dump('quota.config', '');

reset search_path;
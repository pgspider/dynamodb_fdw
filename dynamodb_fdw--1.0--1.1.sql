/* contrib/dynamodb_fdw/dynamodb_fdw--1.0--1.1.sql */

CREATE OR REPLACE FUNCTION dynamodb_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;


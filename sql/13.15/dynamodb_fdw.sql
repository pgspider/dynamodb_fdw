\set ECHO none
\ir sql/parameters.conf
\set ECHO all


--Testcase 1:
CREATE EXTENSION IF NOT EXISTS dynamodb_fdw;
--Testcase 2:
CREATE SERVER dynamodb_server FOREIGN DATA WRAPPER dynamodb_fdw
  OPTIONS (endpoint :DYNAMODB_ENDPOINT);
-- Get version
--Testcase 42:
SELECT * FROM public.dynamodb_fdw_version();
--Testcase 43:
SELECT dynamodb_fdw_version();
-- ====================================================================
-- Check that userid to use when querying the remote table is correctly
-- propagated into foreign rels.
-- ====================================================================
-- create empty_owner without access information to detect incorrect UserID.
--Testcase 3:
CREATE ROLE empty_owner LOGIN SUPERUSER;
--Testcase 4:
SET ROLE empty_owner;

--Testcase 5:
CREATE FOREIGN TABLE example1 (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'connection_tbl', partition_key 'artist', sort_key 'songtitle');

--Testcase 6:
CREATE VIEW v4 AS SELECT * FROM example1;
-- If undefine user owner, postgres core defaults to using the current user to query.
-- For Foreign Scan, Foreign Modify.
--Testcase 7:
SELECT * FROM v4;
--Testcase 8:
INSERT INTO v4 VALUES ('1', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 9:
UPDATE v4 SET albumtitle = 'RECORD UPDATED';
--Testcase 10:
DELETE FROM v4;

--Testcase 11:
CREATE ROLE regress_view_owner_another;
--Testcase 12:
ALTER VIEW v4 OWNER TO regress_view_owner_another;
--Testcase 13:
ALTER FOREIGN TABLE example1 OWNER TO regress_view_owner_another;
--Testcase 14:
GRANT SELECT ON example1 TO regress_view_owner_another;
--Testcase 15:
GRANT INSERT ON example1 TO regress_view_owner_another;
--Testcase 16:
GRANT UPDATE ON example1 TO regress_view_owner_another;
--Testcase 17:
GRANT DELETE ON example1 TO regress_view_owner_another;

-- It fails as expected due to the lack of a user mapping for that user.
-- For Foreign Scan, Foreign Modify.
--Testcase 18:
SELECT * FROM v4;
--Testcase 19:
INSERT INTO v4 VALUES ('1', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 20:
UPDATE v4 SET albumtitle = 'RECORD UPDATED';
--Testcase 21:
DELETE FROM v4;

-- Identify the correct user, but it fails due to the lack access informations.
--Testcase 22:
CREATE USER MAPPING FOR regress_view_owner_another SERVER dynamodb_server;
-- For Foreign Scan, Foreign Modify.
--Testcase 23:
SELECT * FROM v4;
--Testcase 24:
INSERT INTO v4 VALUES ('1', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 25:
UPDATE v4 SET albumtitle = 'RECORD UPDATED';
--Testcase 26:
DELETE FROM v4;

--Testcase 27:
DROP USER MAPPING FOR regress_view_owner_another SERVER dynamodb_server;

-- Should not get that error once a user mapping is created and have enough information.
--Testcase 28:
CREATE USER MAPPING FOR regress_view_owner_another SERVER dynamodb_server OPTIONS (user :DYNAMODB_USER, password :DYNAMODB_PASSWORD);
-- For Foreign Scan, Foreign Modify.
--Testcase 29:
SELECT * FROM v4;
--Testcase 30:
INSERT INTO v4 VALUES ('1', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 31:
UPDATE v4 SET albumtitle = 'RECORD UPDATED';
--Testcase 32:
DELETE FROM v4;

-- Clean
--Testcase 33:
DROP VIEW v4;
--Testcase 34:
DROP USER MAPPING FOR regress_view_owner_another SERVER dynamodb_server;
--Testcase 35:
DROP OWNED BY regress_view_owner_another;
--Testcase 36:
DROP OWNED BY empty_owner;
--Testcase 37:
DROP ROLE regress_view_owner_another;
--Testcase 38:
-- current user cannot be dropped
--Testcase 39:
RESET ROLE;
--Testcase 40:
DROP ROLE empty_owner;
--Testcase 41:
DROP EXTENSION dynamodb_fdw CASCADE;

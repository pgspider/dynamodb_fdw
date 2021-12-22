\set ECHO none
\ir sql/parameters.conf
\set ECHO all


--Testcase 1:
CREATE EXTENSION IF NOT EXISTS dynamodb_fdw;
--Testcase 2:
CREATE SERVER dynamodb_server FOREIGN DATA WRAPPER dynamodb_fdw
  OPTIONS (endpoint :DYNAMODB_ENDPOINT);
--Testcase 3:
CREATE USER MAPPING FOR public SERVER dynamodb_server 
  OPTIONS (user :DYNAMODB_USER, password :DYNAMODB_PASSWORD);


--Testcase 4:
CREATE FOREIGN TABLE delete_test (id SERIAL, a INT, b text) SERVER dynamodb_server OPTIONS (table_name 'delete_test', partition_key 'id');

--Testcase 5:
EXPLAIN VERBOSE
INSERT INTO delete_test (a) VALUES (10);
--Testcase 6:
INSERT INTO delete_test (a) VALUES (10);

--Testcase 7:
EXPLAIN VERBOSE
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));
--Testcase 8:
INSERT INTO delete_test (a, b) VALUES (50, repeat('x', 10000));

--Testcase 9:
EXPLAIN VERBOSE
INSERT INTO delete_test (a) VALUES (100);
--Testcase 10:
INSERT INTO delete_test (a) VALUES (100);


-- allow an alias to be specified for DELETE's target table
--Testcase 11:
EXPLAIN VERBOSE
DELETE FROM delete_test AS dt WHERE dt.a > 75;
--Testcase 12:
DELETE FROM delete_test AS dt WHERE dt.a > 75;


-- if an alias is specified, don't allow the original table name
-- to be referenced
--Testcase 13:
EXPLAIN VERBOSE
DELETE FROM delete_test dt WHERE dt.a > 25;

--Testcase 14:
DELETE FROM delete_test dt WHERE dt.a > 25;

--Testcase 15:
EXPLAIN VERBOSE
SELECT id, a, char_length(b) FROM delete_test;
--Testcase 16:
SELECT id, a, char_length(b) FROM delete_test;


-- delete a row with a TOASTed value
--Testcase 17:
EXPLAIN VERBOSE
DELETE FROM delete_test WHERE a > 25;
--Testcase 18:
DELETE FROM delete_test WHERE a > 25;

--Testcase 19:
EXPLAIN VERBOSE
SELECT id, a, char_length(b) FROM delete_test;
--Testcase 20:
SELECT id, a, char_length(b) FROM delete_test;

--Testcase 21:
DROP FOREIGN TABLE delete_test;


--Testcase 22:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 23:
DROP EXTENSION dynamodb_fdw CASCADE;

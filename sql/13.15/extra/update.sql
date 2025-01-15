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

--
-- UPDATE syntax tests
--

--Testcase 4:
CREATE FOREIGN TABLE update_test ("ID" INT, a INT, b INT, c TEXT)
  SERVER dynamodb_server OPTIONS (table_name 'update_test', partition_key 'ID');

--Testcase 6:
EXPLAIN VERBOSE
INSERT INTO update_test VALUES (1, 5, 10, 'foo');
--Testcase 7:
INSERT INTO update_test VALUES (1, 5, 10, 'foo');

--Testcase 8:
EXPLAIN VERBOSE
INSERT INTO update_test("ID", b, a) VALUES (2, 15, 10);
--Testcase 9:
INSERT INTO update_test("ID", b, a) VALUES (2, 15, 10);

--Testcase 10:
EXPLAIN VERBOSE
SELECT * FROM update_test;
--Testcase 11:
SELECT * FROM update_test;

--Testcase 12:
EXPLAIN VERBOSE
UPDATE update_test SET a = DEFAULT, b = DEFAULT;
--Testcase 13:
UPDATE update_test SET a = DEFAULT, b = DEFAULT;

--Testcase 14:
SELECT * FROM update_test;

-- aliases for the UPDATE target table
--Testcase 15:
EXPLAIN VERBOSE
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;
--Testcase 16:
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

--Testcase 17:
EXPLAIN VERBOSE
SELECT * FROM update_test;
--Testcase 18:
SELECT * FROM update_test;

--Testcase 19:
EXPLAIN VERBOSE
UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;
--Testcase 20:
UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

--Testcase 21:
SELECT * FROM update_test;


--
-- Test VALUES in FROM
--
--Testcase 22:
EXPLAIN VERBOSE
UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;
--Testcase 23:
UPDATE update_test SET a=v.i FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--Testcase 24:
SELECT * FROM update_test;

-- fail, wrong data type:
--Testcase 25:
EXPLAIN VERBOSE
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;
--Testcase 26:
UPDATE update_test SET a = v.* FROM (VALUES(100, 20)) AS v(i, j)
  WHERE update_test.b = v.j;

--
-- Test multiple-set-clause syntax
--
--Testcase 27:
EXPLAIN VERBOSE
INSERT INTO update_test SELECT "ID"+5,a,b+1,c FROM update_test;
--Testcase 28:
INSERT INTO update_test SELECT "ID"+5,a,b+1,c FROM update_test;

--Testcase 29:
SELECT * FROM update_test;

--Testcase 30:
EXPLAIN VERBOSE
UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
--Testcase 31:
UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';

--Testcase 32:
SELECT * FROM update_test;

--Testcase 33:
EXPLAIN VERBOSE
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
--Testcase 34:
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;

--Testcase 35:
SELECT * FROM update_test;

-- fail, multi assignment to same column:
--Testcase 36:
EXPLAIN VERBOSE
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;
--Testcase 37:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;
--Testcase 38:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;



-- uncorrelated sub-select:
--Testcase 39:
EXPLAIN VERBOSE
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
--Testcase 40:
UPDATE update_test
  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
  WHERE a = 100 AND b = 20;
--Testcase 41:
SELECT * FROM update_test;

-- correlated sub-select:
--Testcase 42:
EXPLAIN VERBOSE
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
--Testcase 43:
UPDATE update_test o
  SET (b,a) = (select a+1,b from update_test i
               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);

--Testcase 44:
SELECT * FROM update_test;

-- fail, multiple rows supplied:
--Testcase 45:
EXPLAIN VERBOSE
UPDATE update_test SET (b,a) = (select a+1,b from update_test);
--Testcase 46:
UPDATE update_test SET (b,a) = (select a+1,b from update_test);

-- set to null if no rows supplied:
--Testcase 47:
EXPLAIN VERBOSE
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;
--Testcase 48:
UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
  WHERE a = 11;

--Testcase 49:
SELECT * FROM update_test;

-- *-expansion should work in this context:
--Testcase 50:
EXPLAIN VERBOSE
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;
--Testcase 51:
UPDATE update_test SET (a,b) = ROW(v.*) FROM (VALUES(21, 100)) AS v(i, j)
  WHERE update_test.a = v.i;

-- you might expect this to work, but syntactically it's not a RowExpr:
--Testcase 52:
EXPLAIN VERBOSE
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;
--Testcase 53:
UPDATE update_test SET (a,b) = (v.*) FROM (VALUES(21, 101)) AS v(i, j)
  WHERE update_test.a = v.i;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
--Testcase 54:
EXPLAIN VERBOSE
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;
--Testcase 55:
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;



-- Make sure that we can update to a TOASTed value.
--Testcase 56:
EXPLAIN VERBOSE
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
--Testcase 57:
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';

--Testcase 58:
EXPLAIN VERBOSE
SELECT a, b, char_length(c) FROM update_test;
--Testcase 59:
SELECT a, b, char_length(c) FROM update_test;



-- Check multi-assignment with a Result node to handle a one-time filter.
--Testcase 60:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 61:
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--Testcase 62:
SELECT a, b, char_length(c) FROM update_test;

/* Skip, dynamodb fdw does not support ON CONFLICT DO UPDATE
--Testcase 5:
CREATE FOREIGN TABLE upsert_test (a INT, b TEXT)
  SERVER dynamodb_server OPTIONS (table_name 'upsert_test', partition_key 'a');

-- Test ON CONFLICT DO UPDATE
--Testcase 63:
EXPLAIN VERBOSE
INSERT INTO upsert_test VALUES(1, 'Boo'), (3, 'Zoo');
--Testcase 64:
INSERT INTO upsert_test VALUES(1, 'Boo'), (3, 'Zoo');

-- uncorrelated  sub-select:
--Testcase 65:
EXPLAIN VERBOSE
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;
--Testcase 66:
WITH aaa AS (SELECT 1 AS a, 'Foo' AS b) INSERT INTO upsert_test
  VALUES (1, 'Bar') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b, a FROM aaa) RETURNING *;

-- correlated sub-select:
--Testcase 67:
EXPLAIN VERBOSE
INSERT INTO upsert_test VALUES (1, 'Baz'), (3, 'Zaz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;
--Testcase 68:
INSERT INTO upsert_test VALUES (1, 'Baz'), (3, 'Zaz') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Correlated', a from upsert_test i WHERE i.a = upsert_test.a)
  RETURNING *;

-- correlated sub-select (EXCLUDED.* alias):
--Testcase 69:
EXPLAIN VERBOSE
INSERT INTO upsert_test VALUES (1, 'Bat'), (3, 'Zot') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;
--Testcase 70:
INSERT INTO upsert_test VALUES (1, 'Bat'), (3, 'Zot') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING *;


-- ON CONFLICT using system attributes in RETURNING, testing both the
-- inserting and updating paths. See bug report at:
-- https://www.postgresql.org/message-id/73436355-6432-49B1-92ED-1FE4F7E7E100%40finefun.com.au
--Testcase 71:
EXPLAIN VERBOSE
INSERT INTO upsert_test VALUES (2, 'Beeble') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = pg_current_xact_id()::xid AS xmin_correct, xmax = 0 AS xmax_correct;
--Testcase 72:
INSERT INTO upsert_test VALUES (2, 'Beeble') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = pg_current_xact_id()::xid AS xmin_correct, xmax = 0 AS xmax_correct;

-- currently xmax is set after a conflict - that's probably not good,
-- but it seems worthwhile to have to be explicit if that changes.
--Testcase 73:
EXPLAIN VERBOSE
INSERT INTO upsert_test VALUES (2, 'Brox') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = pg_current_xact_id()::xid AS xmin_correct, xmax = pg_current_xact_id()::xid AS xmax_correct;
--Testcase 74:
INSERT INTO upsert_test VALUES (2, 'Brox') ON CONFLICT(a)
  DO UPDATE SET (b, a) = (SELECT b || ', Excluded', a from upsert_test i WHERE i.a = excluded.a)
  RETURNING tableoid::regclass, xmin = pg_current_xact_id()::xid AS xmin_correct, xmax = pg_current_xact_id()::xid AS xmax_correct;

--Testcase 76:
DROP FOREIGN TABLE upsert_test;
*/

--Testcase 75:
DROP FOREIGN TABLE update_test;

--Testcase 77:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 78:
DROP EXTENSION dynamodb_fdw CASCADE;

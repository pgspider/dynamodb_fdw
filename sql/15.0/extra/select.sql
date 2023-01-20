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
CREATE FOREIGN TABLE onek (
  unique1   int4,
  unique2   int4,
  two       int4,
  four      int4,
  ten       int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd       int4,
  even      int4,
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER dynamodb_server OPTIONS (table_name 'onek', partition_key 'unique1');

--Testcase 5:
CREATE FOREIGN TABLE onek2 (
  unique1   int4,
  unique2   int4,
  two       int4,
  four      int4,
  ten       int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd       int4,
  even      int4,
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER dynamodb_server OPTIONS (table_name 'onek', partition_key 'unique1');

-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
--Testcase 6:
EXPLAIN VERBOSE
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;
--Testcase 7:
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
--Testcase 8:
EXPLAIN VERBOSE
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;
--Testcase 9:
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;  

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
--Testcase 10:
EXPLAIN VERBOSE
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;
--Testcase 11:
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;
  
--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
--Testcase 12:
EXPLAIN VERBOSE
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;
--Testcase 13:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
--Testcase 14:
EXPLAIN VERBOSE
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;
--Testcase 15:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
--Testcase 16:
EXPLAIN VERBOSE
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;
--Testcase 17:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
--Testcase 18:
EXPLAIN VERBOSE
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;
--Testcase 19:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
-- ANALYZE onek2;

SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
--Testcase 20:
EXPLAIN VERBOSE
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10;
--Testcase 21:
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
--Testcase 22:
EXPLAIN VERBOSE
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;
--Testcase 23:
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
--Testcase 24:
EXPLAIN VERBOSE
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980;
--Testcase 25:
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;

--
-- Test some cases involving whole-row Var referencing a subquery
--
--Testcase 28:
EXPLAIN VERBOSE
select foo from (select 1 offset 0) as foo;
--Testcase 29:
select foo from (select 1 offset 0) as foo;

--Testcase 30:
EXPLAIN VERBOSE
select foo from (select null offset 0) as foo;
--Testcase 31:
select foo from (select null offset 0) as foo;

--Testcase 32:
EXPLAIN VERBOSE
select foo from (select 'xyzzy',1,null offset 0) as foo;
--Testcase 33:
select foo from (select 'xyzzy',1,null offset 0) as foo;


--
-- Test VALUES lists
--
--Testcase 34:
EXPLAIN VERBOSE
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;
--Testcase 35:
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;

-- a more complex case
-- looks like we're coding lisp :-)
--Testcase 36:
EXPLAIN VERBOSE
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i;
--Testcase 37:
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i;

-- try VALUES in a subquery
--Testcase 38:
EXPLAIN VERBOSE
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;
--Testcase 39:
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;

-- DynamoDB does not allow to create table with no columns
-- corner case: VALUES with no columns
-- CREATE TEMP TABLE nocols();
-- INSERT INTO nocols DEFAULT VALUES;
-- SELECT * FROM nocols n, LATERAL (VALUES(n.*)) v;

--
-- Test ORDER BY options
--

--Testcase 40:
CREATE FOREIGN TABLE foo (key_dy int, f1 int) SERVER dynamodb_server OPTIONS (table_name 'foo_select',  partition_key 'key_dy');

--INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);
--Testcase 41:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1;
--Testcase 42:
SELECT * FROM foo ORDER BY f1;

--Testcase 43:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing
--Testcase 44:
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing

--Testcase 45:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 46:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;

--Testcase 47:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 48:
SELECT * FROM foo ORDER BY f1 DESC;

--Testcase 49:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;
--Testcase 50:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;


-- check if indexscans do the right things
--Testcase 51:
CREATE INDEX fooi ON foo (f1);
SET enable_sort = false;

--Testcase 52:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1;
--Testcase 53:
SELECT * FROM foo ORDER BY f1;

--Testcase 54:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 55:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;

--Testcase 56:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 57:
SELECT * FROM foo ORDER BY f1 DESC;

--Testcase 58:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;
--Testcase 59:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--Testcase 60:
DROP INDEX fooi;
--Testcase 61:
CREATE INDEX fooi ON foo (f1 DESC);

--Testcase 62:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1;
--Testcase 63:
SELECT * FROM foo ORDER BY f1;

--Testcase 64:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 65:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;

--Testcase 66:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 67:
SELECT * FROM foo ORDER BY f1 DESC;

--Testcase 68:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;
--Testcase 69:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--Testcase 70:
DROP INDEX fooi;
--Testcase 71:
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

--Testcase 72:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1;
--Testcase 73:
SELECT * FROM foo ORDER BY f1;

--Testcase 74:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 75:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;

--Testcase 76:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 77:
SELECT * FROM foo ORDER BY f1 DESC;

--Testcase 78:
EXPLAIN VERBOSE
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;
--Testcase 79:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--
-- Test planning of some cases with partial indexes
--

-- partial index is usable
--Testcase 80:
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 81:
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- actually run the query with an analyze to use the partial index
--Testcase 82:
explain (costs off, analyze on, timing off, summary off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 83:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 84:
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- partial index predicate implies clause, so no need for retest
--Testcase 85:
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 86:
select * from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 87:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 88:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
-- but if it's an update target, must retest anyway
--Testcase 89:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
--Testcase 90:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
-- partial index is not applicable
--Testcase 91:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
--Testcase 92:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
-- partial index implies clause, but bitmap scan must recheck predicate anyway
SET enable_indexscan TO off;
--Testcase 93:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 94:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
RESET enable_indexscan;
-- check multi-index cases too
--Testcase 95:
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
--Testcase 96:
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
--Testcase 97:
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;
--Testcase 98:
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;

--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
--Testcase 99:
EXPLAIN VERBOSE
SELECT 1 AS x ORDER BY x;
--Testcase 100:
SELECT 1 AS x ORDER BY x;

--Testcase 101:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 102:
DROP EXTENSION dynamodb_fdw CASCADE;



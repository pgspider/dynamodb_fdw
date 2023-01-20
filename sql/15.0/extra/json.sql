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


--constructors
-- row_to_json
--Testcase 4:
CREATE FOREIGN TABLE rows ("ID" int, x int, y text)
 SERVER dynamodb_server OPTIONS (table_name 'rows_json', partition_key 'ID');

--Testcase 5:
EXPLAIN VERBOSE
SELECT row_to_json(q,true)
FROM rows q;
--Testcase 6:
SELECT row_to_json(q,true)
FROM rows q;

--Testcase 7:
EXPLAIN VERBOSE
SELECT row_to_json(row((select array_agg(x) as d from generate_series(5,10) x)),false);
--Testcase 8:
SELECT row_to_json(row((select array_agg(x) as d from generate_series(5,10) x)),false);

--json_agg
--Testcase 9:
EXPLAIN VERBOSE
SELECT json_agg(q ORDER BY x, y)
  FROM rows q;
--Testcase 10:
SELECT json_agg(q ORDER BY x, y)
  FROM rows q;

--Testcase 11:
EXPLAIN VERBOSE
UPDATE rows SET x = NULL WHERE x = 1;
--Testcase 12:
UPDATE rows SET x = NULL WHERE x = 1;

--Testcase 13:
EXPLAIN VERBOSE
SELECT json_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;
--Testcase 14:
SELECT json_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;

-- json extraction functions
--Testcase 15:
CREATE FOREIGN TABLE test_json ("ID" int, json_type text, test_json json)
 SERVER dynamodb_server OPTIONS (table_name 'test_json', partition_key 'ID');

--Testcase 16:
EXPLAIN VERBOSE
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'scalar';
--Testcase 17:
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'scalar';

--Testcase 18:
EXPLAIN VERBOSE
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'array';
--Testcase 19:
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'array';

--Testcase 20:
EXPLAIN VERBOSE
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'object';
--Testcase 21:
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'object';

--Testcase 22:
EXPLAIN VERBOSE
SELECT test_json->'field2'
FROM test_json
WHERE json_type = 'object';
--Testcase 23:
SELECT test_json->'field2'
FROM test_json
WHERE json_type = 'object';

--Testcase 24:
EXPLAIN VERBOSE
SELECT test_json->>'field2'
FROM test_json
WHERE json_type = 'object';
--Testcase 25:
SELECT test_json->>'field2'
FROM test_json
WHERE json_type = 'object';

--Testcase 26:
EXPLAIN VERBOSE
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'scalar';
--Testcase 27:
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'scalar';

--Testcase 28:
EXPLAIN VERBOSE
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'array';
--Testcase 29:
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'array';

--Testcase 30:
EXPLAIN VERBOSE
SELECT test_json -> -1
FROM test_json
WHERE json_type = 'array';
--Testcase 31:
SELECT test_json -> -1
FROM test_json
WHERE json_type = 'array';

--Testcase 32:
EXPLAIN VERBOSE
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'object';
--Testcase 33:
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'object';

--Testcase 34:
EXPLAIN VERBOSE
SELECT test_json->>2
FROM test_json
WHERE json_type = 'array';
--Testcase 35:
SELECT test_json->>2
FROM test_json
WHERE json_type = 'array';

--Testcase 36:
EXPLAIN VERBOSE
SELECT test_json ->> 6 FROM test_json WHERE json_type = 'array';
--Testcase 37:
SELECT test_json ->> 6 FROM test_json WHERE json_type = 'array';

--Testcase 38:
EXPLAIN VERBOSE
SELECT test_json ->> 7 FROM test_json WHERE json_type = 'array';
--Testcase 39:
SELECT test_json ->> 7 FROM test_json WHERE json_type = 'array';

--Testcase 40:
EXPLAIN VERBOSE
SELECT test_json ->> 'field4' FROM test_json WHERE json_type = 'object';
--Testcase 41:
SELECT test_json ->> 'field4' FROM test_json WHERE json_type = 'object';

--Testcase 42:
EXPLAIN VERBOSE
SELECT test_json ->> 'field5' FROM test_json WHERE json_type = 'object';
--Testcase 43:
SELECT test_json ->> 'field5' FROM test_json WHERE json_type = 'object';

--Testcase 44:
EXPLAIN VERBOSE
SELECT test_json ->> 'field6' FROM test_json WHERE json_type = 'object';
--Testcase 45:
SELECT test_json ->> 'field6' FROM test_json WHERE json_type = 'object';

--Testcase 46:
EXPLAIN VERBOSE
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'scalar';
--Testcase 47:
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'scalar';

--Testcase 48:
EXPLAIN VERBOSE
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'array';
--Testcase 49:
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'array';

--Testcase 50:
EXPLAIN VERBOSE
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'object';
--Testcase 51:
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'object';

-- nulls
--Testcase 52:
EXPLAIN VERBOSE
select (test_json->'field3') is null as expect_false
from test_json
where json_type = 'object';
--Testcase 53:
select (test_json->'field3') is null as expect_false
from test_json
where json_type = 'object';

--Testcase 54:
EXPLAIN VERBOSE
select (test_json->>'field3') is null as expect_true
from test_json
where json_type = 'object';
--Testcase 55:
select (test_json->>'field3') is null as expect_true
from test_json
where json_type = 'object';

--Testcase 56:
EXPLAIN VERBOSE
select (test_json->3) is null as expect_false
from test_json
where json_type = 'array';
--Testcase 57:
select (test_json->3) is null as expect_false
from test_json
where json_type = 'array';

--Testcase 58:
EXPLAIN VERBOSE
select (test_json->>3) is null as expect_true
from test_json
where json_type = 'array';
--Testcase 59:
select (test_json->>3) is null as expect_true
from test_json
where json_type = 'array';

-- populate_record
--Testcase 60:
create type jpop as (a text, b int, c timestamp);

--Testcase 61:
CREATE DOMAIN js_int_array_1d  AS int[]   CHECK(array_length(VALUE, 1) = 3);
--Testcase 62:
CREATE DOMAIN js_int_array_2d  AS int[][] CHECK(array_length(VALUE, 2) = 3);

--Testcase 63:
CREATE TYPE jsrec AS (
	i int,
	ia _int4,
	ia1 int[],
	ia2 int[][],
	ia3 int[][][],
	ia1d js_int_array_1d,
	ia2d js_int_array_2d,
	t text,
	ta text[],
	c char(10),
	ca char(10)[],
	ts timestamp,
	js json,
	jsb jsonb,
	jsa json[],
	rec jpop,
	reca jpop[]
);

-- test type info caching in json_populate_record()
--Testcase 64:
CREATE FOREIGN TABLE jspoptest ("ID" int, js json)
 SERVER dynamodb_server OPTIONS (table_name 'jspoptest', partition_key 'ID');

--Testcase 65:
EXPLAIN VERBOSE
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;
--Testcase 66:
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;

--Testcase 67:
DROP TYPE jsrec;
--Testcase 68:
DROP DOMAIN js_int_array_1d;
--Testcase 69:
DROP DOMAIN js_int_array_2d;


--Testcase 70:
CREATE FOREIGN TABLE foo (serial_num int, name text, type text)
 SERVER dynamodb_server OPTIONS (table_name 'foo_json', partition_key 'serial_num');

--Testcase 71:
EXPLAIN VERBOSE
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;
--Testcase 72:
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;

--Testcase 73:
EXPLAIN VERBOSE
SELECT json_object_agg(name, type) FROM foo;
--Testcase 74:
SELECT json_object_agg(name, type) FROM foo;

--Testcase 75:
EXPLAIN VERBOSE
INSERT INTO foo VALUES (999999, NULL, 'bar');
--Testcase 76:
INSERT INTO foo VALUES (999999, NULL, 'bar');

--Testcase 77:
EXPLAIN VERBOSE
SELECT json_object_agg(name, type) FROM foo;
--Testcase 78:
SELECT json_object_agg(name, type) FROM foo;

--Testcase 79:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 80:
DROP EXTENSION dynamodb_fdw CASCADE;






 

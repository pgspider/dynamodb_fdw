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


-- TEST FOR AGGREGATE

--Testcase 4:
CREATE FOREIGN TABLE agg_tbl (
    id int, score float, 
    numbers float[], 
    name text, 
    color int[], 
    active boolean, 
    active_lock_reason text, 
    created_at timestamp, 
    events text[], 
    description text, 
    teams jsonb) 
SERVER dynamodb_server OPTIONS (table_name 'agg_tbl', partition_key 'id');


-- test simple aggregate
--Testcase 5:
EXPLAIN VERBOSE
SELECT var_pop(1.0::float8), var_samp(2.0::float8) FROM agg_tbl;
--Testcase 6:
SELECT var_pop(1.0::float8), var_samp(2.0::float8) FROM agg_tbl;

--Testcase 7:
EXPLAIN VERBOSE
SELECT stddev_pop('nan'::float8), stddev_samp('nan'::float8) FROM agg_tbl;
--Testcase 8:
SELECT stddev_pop('nan'::float8), stddev_samp('nan'::float8) FROM agg_tbl;

--Testcase 9:
EXPLAIN VERBOSE
SELECT var_pop(1.0::float4), var_samp(2.0::float4) FROM agg_tbl;
--Testcase 10:
SELECT var_pop(1.0::float4), var_samp(2.0::float4) FROM agg_tbl;

--Testcase 11:
EXPLAIN VERBOSE
SELECT var_pop('nan'::numeric), var_samp('nan'::numeric) FROM agg_tbl;
--Testcase 12:
SELECT var_pop('nan'::numeric), var_samp('nan'::numeric) FROM agg_tbl;

--Testcase 13:
EXPLAIN VERBOSE
SELECT stddev_pop('nan'::numeric), stddev_samp('nan'::numeric) FROM agg_tbl;
--Testcase 14:
SELECT stddev_pop('nan'::numeric), stddev_samp('nan'::numeric) FROM agg_tbl;

--Testcase 15:
EXPLAIN VERBOSE SELECT count(*) FROM agg_tbl;
--Testcase 16:
SELECT count(*) FROM agg_tbl;

--Testcase 17:
EXPLAIN VERBOSE
SELECT count(id) AS cnt FROM agg_tbl;
--Testcase 18:
SELECT count(id) AS cnt FROM agg_tbl;

--Testcase 19:
EXPLAIN VERBOSE SELECT count(DISTINCT id) AS cnt FROM agg_tbl;
--Testcase 20:
SELECT count(DISTINCT id) AS cnt FROM agg_tbl;


--Testcase 21:
EXPLAIN VERBOSE
SELECT  BIT_AND(color[0]) AS "1", BIT_AND(color[1]) AS "1", BIT_OR(color[0]) AS "?", BIT_OR(color[1]) AS "7" FROM agg_tbl;
--Testcase 22:
SELECT  BIT_AND(color[0]) AS "1", BIT_AND(color[1]) AS "1", BIT_OR(color[0]) AS "?", BIT_OR(color[1]) AS "7" FROM agg_tbl;

--Testcase 23:
EXPLAIN VERBOSE
SELECT BOOL_AND(id > 0), BOOL_AND(NOT id <= 0), BOOL_OR(id = 0), BOOL_OR(NOT id != 0), EVERY(id >= 0), EVERY(NOT id <> 0) FROM agg_tbl;
--Testcase 24:
SELECT BOOL_AND(id > 0), BOOL_AND(NOT id <= 0), BOOL_OR(id = 0), BOOL_OR(NOT id != 0), EVERY(id >= 0), EVERY(NOT id <> 0) FROM agg_tbl;

-- test aggregate and where
--Testcase 25:
EXPLAIN VERBOSE
SELECT max(id), sum(score), avg(score) FROM agg_tbl WHERE id > 0;
--Testcase 26:
SELECT max(id), sum(score), avg(score) FROM agg_tbl WHERE id > 0;

--Testcase 27:
EXPLAIN VERBOSE
SELECT distinct max(id) FROM agg_tbl WHERE id <> 0;
--Testcase 28:
SELECT distinct max(id) FROM agg_tbl WHERE id <> 0;

--Testcase 29:
EXPLAIN VERBOSE
SELECT distinct min(score), max(score) FROM agg_tbl WHERE id != 0;
--Testcase 30:
SELECT distinct min(score), max(score) FROM agg_tbl WHERE id != 0;

--Testcase 31:
EXPLAIN VERBOSE
SELECT min(id) FROM agg_tbl WHERE id >= 0;
--Testcase 32:
SELECT min(id) FROM agg_tbl WHERE id >= 0;

--Testcase 33:
EXPLAIN VERBOSE
SELECT array_agg(name || ':' || description), array_agg(events) FROM agg_tbl WHERE score <> 0.0;
--Testcase 34:
SELECT array_agg(name || ':' || description), array_agg(events) FROM agg_tbl WHERE score <> 0.0;

--Testcase 35:
EXPLAIN VERBOSE
SELECT max(name || description), min (name || description), avg(score) FROM agg_tbl WHERE color[0] IN (21, 210, 121, 211);
--Testcase 36:
SELECT max(name || description), min (name || description), avg(score) FROM agg_tbl WHERE color[0] IN (21, 210, 121, 211);

--Testcase 37:
EXPLAIN VERBOSE
SELECT sum(id)/sum(score), max(name), min(description) FROM agg_tbl WHERE id BETWEEN 0 AND 55666;
--Testcase 38:
SELECT sum(id)/sum(score), max(name), min(description) FROM agg_tbl WHERE id BETWEEN 0 AND 55666;

--Testcase 39:
EXPLAIN VERBOSE
SELECT stddev_pop(score), stddev_samp(score), stddev(score) FROM agg_tbl WHERE id IN (3887, 2891, 2747);
--Testcase 40:
SELECT stddev_pop(score), stddev_samp(score), stddev(score) FROM agg_tbl WHERE id IN (3887, 2891, 2747);

--Testcase 41:
EXPLAIN VERBOSE
SELECT count(DISTINCT id), max(description), min(name) FROM agg_tbl WHERE id <= 586878;
--Testcase 42:
SELECT count(DISTINCT id), max(description), min(name) FROM agg_tbl WHERE id <= 586878;

--Testcase 43:
EXPLAIN VERBOSE
SELECT avg(score), sum(color[0] + color[1] + color[2]), stddev(color[0] + color[1] + color[2]) FROM agg_tbl WHERE color[0] IN (21, 210, 121, 211) AND id > 0;
--Testcase 44:
SELECT avg(score), sum(color[0] + color[1] + color[2]), stddev(color[0] + color[1] + color[2]) FROM agg_tbl WHERE color[0] IN (21, 210, 121, 211) AND id > 0;

--Testcase 45:
EXPLAIN VERBOSE
SELECT id, name, avg(color[0] + color[1] + color[2]) FROM agg_tbl WHERE score BETWEEN -212.23 AND 3313.4 GROUP BY id, name, events HAVING events[0] = 'create' ORDER BY id, name;
--Testcase 46:
SELECT id, name, avg(color[0] + color[1] + color[2]) FROM agg_tbl WHERE score BETWEEN -212.23 AND 3313.4 GROUP BY id, name, events HAVING events[0] = 'create' ORDER BY id, name;

--Testcase 47:
EXPLAIN VERBOSE
SELECT max(numbers[0]) + 22, min(numbers[1])/score FROM agg_tbl WHERE score NOT IN (0, 0.0, 0.21) GROUP BY score, id HAVING min(id) > 0 ORDER BY id;
--Testcase 48:
SELECT max(numbers[0]) + 22, min(numbers[1])/score FROM agg_tbl WHERE score NOT IN (0, 0.0, 0.21) GROUP BY score, id HAVING min(id) > 0 ORDER BY id;

--Testcase 49:
EXPLAIN VERBOSE
SELECT id, count(*), sum(score) FROM agg_tbl WHERE id > 0 GROUP BY id, name HAVING name IN ('invalid', 'question') ORDER BY id ASC, name DESC;
--Testcase 50:
SELECT id, count(*), sum(score) FROM agg_tbl WHERE id > 0 GROUP BY id, name HAVING name IN ('invalid', 'question') ORDER BY id ASC, name DESC;

--Testcase 51:
EXPLAIN VERBOSE
SELECT id, count(score), sum(DISTINCT score) FROM agg_tbl WHERE description IS NOT NULL GROUP BY id, active HAVING active = true ORDER BY id, active;
--Testcase 52:
SELECT id, count(score), sum(DISTINCT score) FROM agg_tbl WHERE description IS NOT NULL GROUP BY id, active HAVING active = true ORDER BY id, active;

--Testcase 53:
EXPLAIN VERBOSE
SELECT string_agg(name, ':' ORDER BY name), string_agg(description, ',' ORDER BY description) FROM agg_tbl WHERE name IS NOT NULL GROUP BY name HAVING max(id) >= 0 ORDER BY name;
--Testcase 54:
SELECT string_agg(name, ':' ORDER BY name), string_agg(description, ',' ORDER BY description) FROM agg_tbl WHERE name IS NOT NULL GROUP BY name HAVING max(id) >= 0 ORDER BY name;

--Testcase 55:
EXPLAIN VERBOSE
SELECT json_agg((name, '!@!*')), jsonb_agg((name, 'varr')), json_object_agg(id, 'x'), jsonb_object_agg(id, '23') FROM agg_tbl WHERE description IS NOT NULL GROUP BY description HAVING min(id) <> 0 ORDER BY description;
--Testcase 56:
SELECT json_agg((name, '!@!*')), jsonb_agg((name, 'varr')), json_object_agg(id, 'x'), jsonb_object_agg(id, '23') FROM agg_tbl WHERE description IS NOT NULL GROUP BY description HAVING min(id) <> 0 ORDER BY description;

--Testcase 57:
EXPLAIN VERBOSE
SELECT count(name), max(description), min(created_at) FROM agg_tbl WHERE active_lock_reason IS NULL GROUP BY name HAVING name != '#!@ADSF' ORDER BY name ASC;
--Testcase 58:
SELECT count(name), max(description), min(created_at) FROM agg_tbl WHERE active_lock_reason IS NULL GROUP BY name HAVING name != '#!@ADSF' ORDER BY name ASC;

--Testcase 59:
EXPLAIN VERBOSE
SELECT created_at, 'aer@#AKSF', 212, count(id) FROM agg_tbl WHERE active_lock_reason IS NOT NULL GROUP BY created_at, active HAVING active != false ORDER BY created_at DESC;
--Testcase 60:
SELECT created_at, 'aer@#AKSF', 212, count(id) FROM agg_tbl WHERE active_lock_reason IS NOT NULL GROUP BY created_at, active HAVING active != false ORDER BY created_at DESC;

--Testcase 61:
EXPLAIN VERBOSE
SELECT avg(numbers[0] + color[0]), avg(numbers[1] + color[1]), avg(numbers[2] + color[2]) FROM agg_tbl WHERE color[0] != 0 GROUP BY name HAVING name != 'WFKAW';
--Testcase 62:
SELECT avg(numbers[0] + color[0]), avg(numbers[1] + color[1]), avg(numbers[2] + color[2]) FROM agg_tbl WHERE color[0] != 0 GROUP BY name HAVING name != 'WFKAW';

--Testcase 63:
EXPLAIN VERBOSE
SELECT events[0], string_agg(events[0], ' ' ORDER BY events[0]) FROM agg_tbl WHERE description IS NOT NULL GROUP BY score, events HAVING score NOT IN (221.12, .21, 313.12) ORDER BY score;
--Testcase 64:
SELECT events[0], string_agg(events[0], ' ' ORDER BY events[0]) FROM agg_tbl WHERE description IS NOT NULL GROUP BY score, events HAVING score NOT IN (221.12, .21, 313.12) ORDER BY score;

--Testcase 65:
EXPLAIN VERBOSE
SELECT string_agg(name, ' '), string_agg(description, ' ') FROM agg_tbl WHERE events[0] IN (SELECT events[0] FROM agg_tbl WHERE id > 0);
--Testcase 66:
SELECT string_agg(name, ' '), string_agg(description, ' ') FROM agg_tbl WHERE events[0] IN (SELECT events[0] FROM agg_tbl WHERE id > 0);

--Testcase 67:
EXPLAIN VERBOSE
SELECT max(name), min(name), max(description), min(description) FROM agg_tbl WHERE score <> ALL(SELECT id FROM agg_tbl);
--Testcase 68:
SELECT max(name), min(name), max(description), min(description) FROM agg_tbl WHERE score <> ALL(SELECT id FROM agg_tbl);


-- test size() function pushdown in WHERE clause 
--Testcase 69:
EXPLAIN VERBOSE
SELECT * FROM agg_tbl WHERE size(id) > 0;
--Testcase 70:
SELECT * FROM agg_tbl WHERE size(id) > 0;

--Testcase 71:
EXPLAIN VERBOSE
SELECT * FROM agg_tbl WHERE size(active) > size(active_lock_reason);
--Testcase 72:
SELECT * FROM agg_tbl WHERE size(active) > size(active_lock_reason);

--Testcase 73:
EXPLAIN VERBOSE
SELECT * FROM agg_tbl WHERE size(id+score) > 0;
--Testcase 74:
SELECT * FROM agg_tbl WHERE size(id+score) > 0;


--Testcase 75:
DROP FOREIGN TABLE agg_tbl;


-- TEST FOR element of array

--Testcase 76:
CREATE FOREIGN TABLE array_test (
    key_dy int, 
    array_n int[], 
    array_s text[], 
    array_f float[], 
    array_jn jsonb, 
    array_js jsonb, 
    array_jf jsonb) 
SERVER dynamodb_server OPTIONS (table_name 'array_test', partition_key 'key_dy');


--Testcase 77:
EXPLAIN VERBOSE 
INSERT INTO array_test (key_dy, array_n[1:5], array_s[1:1], array_f, array_jn, array_js, array_jf) VALUES (11, '{1,2,3,4,5}', '{"AAAAA33250", "AAAAAAAAAAAAAAAAAAA85420", "AAAAAAAAAAA33576"}', '{}', '{}', '{}', '{}');
--Testcase 78:
INSERT INTO array_test (key_dy, array_n[1:5], array_s[1:1], array_f, array_jn, array_js, array_jf) VALUES (11, '{1,2,3,4,5}', '{"AAAAA33250", "AAAAAAAAAAAAAAAAAAA85420", "AAAAAAAAAAA33576"}', '{}', '{}', '{}', '{}');

--Testcase 79:
EXPLAIN VERBOSE 
UPDATE array_test SET array_f[0] = '1.1';
--Testcase 80:
UPDATE array_test SET array_f[0] = '1.1';

--Testcase 81:
EXPLAIN VERBOSE 
UPDATE array_test SET array_f[1] = '2.2';
--Testcase 82:
UPDATE array_test SET array_f[1] = '2.2';

--Testcase 83:
-- not set value for key, should fail 
EXPLAIN VERBOSE 
INSERT INTO array_test (array_s) VALUES ('{"too long"}');
--Testcase 84:
INSERT INTO array_test (array_s) VALUES ('{"too long"}');

--Testcase 85:
EXPLAIN VERBOSE 
INSERT INTO array_test (key_dy, array_n, array_s[2:2], array_f, array_jn, array_js, array_jf) VALUES (12, '{11,12}', '{"`3gOJ9gOJ3"}', '{"63764.41485008047", "-38626.05345625236"}', '{"key1": "|2FSo3FSo4*", "key2":[70356, 24708, 68429, -84248, -83370]}', '{"key1": "~4YMW4YMW6|", "key2":["`3wSi5wSi8!"]}', '{"key1": "~4YMW4YMW6|", "key2":{"key22": [-76224.64548801386, 28668.41368482083]}}');
--Testcase 86:
INSERT INTO array_test (key_dy, array_n, array_s[2:2], array_f, array_jn, array_js, array_jf) VALUES (12, '{11,12}', '{"`3gOJ9gOJ3"}', '{"63764.41485008047", "-38626.05345625236"}', '{"key1": "|2FSo3FSo4*", "key2":[70356, 24708, 68429, -84248, -83370]}', '{"key1": "~4YMW4YMW6|", "key2":["`3wSi5wSi8!"]}', '{"key1": "~4YMW4YMW6|", "key2":{"key22": [-76224.64548801386, 28668.41368482083]}}');

--Testcase 87:
EXPLAIN VERBOSE 
INSERT INTO array_test (key_dy, array_n, array_s, array_f) VALUES (13, '{3,4}', '{"AAAAAA98232", "AAAA49534", "AAAAAAAAAAA21658"}', '{-79846.26103075403}');
--Testcase 88:
INSERT INTO array_test (key_dy, array_n, array_s, array_f) VALUES (13, '{3,4}', '{"AAAAAA98232", "AAAA49534", "AAAAAAAAAAA21658"}', '{-79846.26103075403}');

--Testcase 89:
EXPLAIN VERBOSE 
INSERT INTO array_test (key_dy, array_n[2]) VALUES(14, now());  -- error, type mismatch
--Testcase 90:
INSERT INTO array_test (key_dy, array_n[2]) VALUES(14, now());  -- error, type mismatch

--Testcase 91:
EXPLAIN VERBOSE 
INSERT INTO array_test (key_dy, array_n[1:2]) VALUES(15, now());  -- error, type mismatch
--Testcase 92:
INSERT INTO array_test (key_dy, array_n[1:2]) VALUES(15, now());  -- error, type mismatch

--Testcase 93:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 94:
SELECT * FROM array_test;

--Testcase 95:
EXPLAIN VERBOSE 
SELECT array_test.array_n[1], array_test.array_s[1], array_test.array_f[1], array_test.array_jn->'key2'->1, array_test.array_js->'key2'->0 FROM array_test;
--Testcase 96:
SELECT array_test.array_n[1], array_test.array_s[1], array_test.array_f[1], array_test.array_jn->'key2'->1, array_test.array_js->'key2'->0 FROM array_test;

--Testcase 97:
EXPLAIN VERBOSE 
SELECT array_n[1], array_s[1], array_f[1], array_jn->'key2'->1, array_js->'key2'->0,  array_jf->'key2'->'key22'->1 FROM array_test;
--Testcase 98:
SELECT array_n[1], array_s[1], array_f[1], array_jn->'key2'->1, array_js->'key2'->0,  array_jf->'key2'->'key22'->1 FROM array_test;

--Testcase 99:
EXPLAIN VERBOSE 
SELECT array_n[1:3], array_s[1:1], array_f[1:2], array_jn->'key2'->1 FROM array_test;
--Testcase 100:
SELECT array_n[1:3], array_s[1:1], array_f[1:2], array_jn->'key2'->1 FROM array_test;

--Testcase 101:
EXPLAIN VERBOSE 
SELECT array_ndims(array_n) AS a, array_ndims(array_s) AS b, array_ndims(array_f) AS c FROM array_test;
--Testcase 102:
SELECT array_ndims(array_n) AS a, array_ndims(array_s) AS b, array_ndims(array_f) AS c FROM array_test;

--Testcase 103:
EXPLAIN VERBOSE 
SELECT array_dims(array_n) AS a,array_dims(array_s) AS b,array_dims(array_f) AS c FROM array_test;
--Testcase 104:
SELECT array_dims(array_n) AS a,array_dims(array_s) AS b,array_dims(array_f) AS c FROM array_test;

-- returns nothing
--Testcase 105:
EXPLAIN VERBOSE 
SELECT * FROM array_test WHERE array_n[1] < 5 and array_s = '{"foobar"}'::text[];
--Testcase 106:
SELECT * FROM array_test WHERE array_n[1] < 5 and array_s = '{"foobar"}'::text[];

--Testcase 107:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[1:2] = '{16,25}' WHERE NOT array_n = '{}'::int[];
--Testcase 108:
UPDATE array_test SET array_n[1:2] = '{16,25}' WHERE NOT array_n = '{}'::int[];

--Testcase 109:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[1:2] = '{113, 117}', array_f[1:2]= '{142.23, 147.233}' WHERE array_dims(array_f) = '[1:5]';
--Testcase 110:
UPDATE array_test SET array_n[1:2] = '{113, 117}', array_f[1:2]= '{142.23, 147.233}' WHERE array_dims(array_f) = '[1:5]';

--Testcase 111:
EXPLAIN VERBOSE 
UPDATE array_test SET array_s[2:2] = '{"new_word"}' WHERE array_dims(array_s) is not null;
--Testcase 112:
UPDATE array_test SET array_s[2:2] = '{"new_word"}' WHERE array_dims(array_s) is not null;

--Testcase 113:
EXPLAIN VERBOSE 
SELECT array_n, array_s, array_f FROM array_test;
--Testcase 114:
SELECT array_n, array_s, array_f FROM array_test;

--Testcase 115:
EXPLAIN VERBOSE 
INSERT INTO array_test(key_dy, array_n) VALUES(16, '{1,null,3}');
--Testcase 116:
INSERT INTO array_test(key_dy, array_n) VALUES(16, '{1,null,3}');

--Testcase 117:
EXPLAIN VERBOSE 
SELECT array_n FROM array_test;
--Testcase 118:
SELECT array_n FROM array_test;

--Testcase 119:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[4] = NULL WHERE array_n[2] IS NULL;
--Testcase 120:
UPDATE array_test SET array_n[4] = NULL WHERE array_n[2] IS NULL;

--Testcase 121:
EXPLAIN VERBOSE 
SELECT array_n FROM array_test WHERE array_n[2] IS NULL;
--Testcase 122:
SELECT array_n FROM array_test WHERE array_n[2] IS NULL;

--Testcase 123:
EXPLAIN VERBOSE 
DELETE FROM array_test WHERE array_n[2] IS NULL AND array_f IS NULL;
--Testcase 124:
DELETE FROM array_test WHERE array_n[2] IS NULL AND array_f IS NULL;

--Testcase 125:
EXPLAIN VERBOSE 
SELECT array_n, array_s, array_f FROM array_test;
--Testcase 126:
SELECT array_n, array_s, array_f FROM array_test;

-- test mixed slice/scalar subscripting
--Testcase 127:
EXPLAIN VERBOSE 
SELECT '{{1,2,3},{4,5,6},{7,8,9}}'::int[];
--Testcase 128:
SELECT '{{1,2,3},{4,5,6},{7,8,9}}'::int[];

--Testcase 129:
EXPLAIN VERBOSE 
SELECT ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2];
--Testcase 130:
SELECT ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2];

--Testcase 131:
EXPLAIN VERBOSE 
SELECT '{"AAAAA33250", "AAAAAAAAAAAAAAAAAAA85420", "AAAAAAAAAAA33576"}'::text[];
--Testcase 132:
SELECT '{"AAAAA33250", "AAAAAAAAAAAAAAAAAAA85420", "AAAAAAAAAAA33576"}'::text[];

--Testcase 133:
EXPLAIN VERBOSE 
SELECT ('{}'::int[])[1][2][3][4][5][6];
--Testcase 134:
SELECT ('{}'::int[])[1][2][3][4][5][6];

-- NULL index yields NULL when selecting
--Testcase 135:
EXPLAIN VERBOSE 
SELECT ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][NULL][1];
--Testcase 136:
SELECT ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][NULL][1];

--Testcase 137:
EXPLAIN VERBOSE 
SELECT ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][NULL:1][1];
--Testcase 138:
SELECT ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][NULL:1][1];

--Testcase 139:
EXPLAIN VERBOSE 
SELECT ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][1:NULL][1];
--Testcase 140:
SELECT ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][1:NULL][1];

--select slices
--Testcase 141:
EXPLAIN VERBOSE 
SELECT array_n[:3], array_s[:2] FROM array_test;
--Testcase 142:
SELECT array_n[:3], array_s[:2] FROM array_test;

--Testcase 143:
EXPLAIN VERBOSE 
SELECT array_n[2:], array_s[2:] FROM array_test;
--Testcase 144:
SELECT array_n[2:], array_s[2:] FROM array_test;

--Testcase 145:
EXPLAIN VERBOSE 
SELECT array_n[:], array_s[:] FROM array_test;
--Testcase 146:
SELECT array_n[:], array_s[:] FROM array_test;

-- updates
--Testcase 147:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[:3] = '{11, 12, 13}', array_s[:2] = '{{"AAAAAA98232", "AAAAAAAA79710"}, {"AAAAAAAAA53663", "AAAAAAAAAAAAAAA67062"}}' WHERE array_lower(array_n, 1) = 1;
--Testcase 148:
UPDATE array_test SET array_n[:3] = '{11, 12, 13}', array_s[:2] = '{{"AAAAAA98232", "AAAAAAAA79710"}, {"AAAAAAAAA53663", "AAAAAAAAAAAAAAA67062"}}' WHERE array_lower(array_n, 1) = 1;

--Testcase 149:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 150:
SELECT * FROM array_test;

--Testcase 151:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[3:] = '{23, 24, 25}', array_s[2:] = '{{"AAAAAAAAA53663", "AAAAAAAAAAAAAAA67062"}, {"AAAAAAAAAAAAAAA73034", "AAAAAAAAAAAAA7929"}}';
--Testcase 152:
UPDATE array_test SET array_n[3:] = '{23, 24, 25}', array_s[2:] = '{{"AAAAAAAAA53663", "AAAAAAAAAAAAAAA67062"}, {"AAAAAAAAAAAAAAA73034", "AAAAAAAAAAAAA7929"}}';

--Testcase 153:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 154:
SELECT * FROM array_test;

--Testcase 155:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[:] = '{11, 12, 13, 14, 15}';
--Testcase 156:
UPDATE array_test SET array_n[:] = '{11, 12, 13, 14, 15}';

--Testcase 157:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 158:
SELECT * FROM array_test;

--Testcase 159:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[:] = '{23, 24, 25}';  -- fail, too small
--Testcase 160:
UPDATE array_test SET array_n[:] = '{23, 24, 25}';  -- fail, too small

--Testcase 161:
EXPLAIN VERBOSE 
INSERT INTO array_test VALUES(17, NULL, NULL);
--Testcase 162:
INSERT INTO array_test VALUES(17, NULL, NULL);

--Testcase 163:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[:] = '{11, 12, 13, 14, 15}';  -- fail, no good with null
--Testcase 164:
UPDATE array_test SET array_n[:] = '{11, 12, 13, 14, 15}';  -- fail, no good with null

--valid insert null
--Testcase 165:
EXPLAIN VERBOSE 
INSERT INTO array_test VALUES(18, NULL, NULL, NULL, NULL, NULL, NULL);
--Testcase 166:
INSERT INTO array_test VALUES(18, NULL, NULL, NULL, NULL, NULL, NULL);

--select and condition =,!=,<>,<,>,<=,>=
--Testcase 167:
EXPLAIN VERBOSE 
SELECT array_n, array_f, array_s FROM array_test WHERE array_n = '{12, 51, 88, 64, 8}';
--Testcase 168:
SELECT array_n, array_f, array_s FROM array_test WHERE array_n = '{12, 51, 88, 64, 8}';

--Testcase 169:
EXPLAIN VERBOSE 
SELECT array_n, array_f, array_s FROM array_test WHERE array_n != '{12, 51, 88, 64, 8}';
--Testcase 170:
SELECT array_n, array_f, array_s FROM array_test WHERE array_n != '{12, 51, 88, 64, 8}';

--Testcase 171:
EXPLAIN VERBOSE 
SELECT * FROM array_test WHERE array_f <> '{515.221, 545.5, -2.12}';
--Testcase 172:
SELECT * FROM array_test WHERE array_f <> '{515.221, 545.5, -2.12}';

--Testcase 173:
EXPLAIN VERBOSE 
SELECT key_dy, array_n, array_s, array_f, array_jn, array_js, array_jf FROM array_test WHERE array_n < '{1232, 5121, 8438, 644, 83}';
--Testcase 174:
SELECT key_dy, array_n, array_s, array_f, array_jn, array_js, array_jf FROM array_test WHERE array_n < '{1232, 5121, 8438, 644, 83}';

--Testcase 175:
EXPLAIN VERBOSE 
SELECT array_s, array_f, array_jn FROM array_test WHERE array_s > '{"$"}';
--Testcase 176:
SELECT array_s, array_f, array_jn FROM array_test WHERE array_s > '{"$"}';

--Testcase 177:
EXPLAIN VERBOSE 
SELECT array_js->'key1', array_js->'key2', array_js->'key2'->0 FROM array_test WHERE (array_js->'key2'->0)::text != '$';
--Testcase 178:
SELECT array_js->'key1', array_js->'key2', array_js->'key2'->0 FROM array_test WHERE (array_js->'key2'->0)::text != '$';

--Testcase 179:
EXPLAIN VERBOSE 
SELECT array_jn->'key1', array_jn->'key2'->0 FROM array_test WHERE (array_jn->'key2'->0)::int >= -122;
--Testcase 180:
SELECT array_jn->'key1', array_jn->'key2'->0 FROM array_test WHERE (array_jn->'key2'->0)::int >= -122;

--Testcase 181:
EXPLAIN VERBOSE 
SELECT array_jf->'key1', array_jf->'key2'->'key22' FROM array_test WHERE (array_jf->'key2'->'key22'->0)::float <= 32212.233;
--Testcase 182:
SELECT array_jf->'key1', array_jf->'key2'->'key22' FROM array_test WHERE (array_jf->'key2'->'key22'->0)::float <= 32212.233;

--Testcase 183:
EXPLAIN VERBOSE 
SELECT array_js->'key1', array_js->'key2', array_js#>'{key2, 0}' FROM array_test WHERE array_js#>>'{key2, 0}' != '$';
--Testcase 184:
SELECT array_js->'key1', array_js->'key2', array_js#>'{key2, 0}' FROM array_test WHERE array_js#>>'{key2, 0}' != '$';

--Testcase 185:
EXPLAIN VERBOSE 
SELECT array_jn->'key1', array_jn#>'{key2, 0}' FROM array_test WHERE (array_jn#>>'{key2, 0}')::int >= -122;
--Testcase 186:
SELECT array_jn->'key1', array_jn#>'{key2, 0}' FROM array_test WHERE (array_jn#>>'{key2, 0}')::int >= -122;

--Testcase 187:
EXPLAIN VERBOSE 
SELECT array_jf->'key1', array_jf#>'{key2,key22,0}' FROM array_test WHERE (array_jf#>>'{key2,key22,0}')::float <= 32212.233;
--Testcase 188:
SELECT array_jf->'key1', array_jf#>'{key2,key22,0}' FROM array_test WHERE (array_jf#>>'{key2,key22,0}')::float <= 32212.233;

--Testcase 189:
EXPLAIN VERBOSE 
SELECT array_n[0], array_s[0], array_f[0] FROM array_test WHERE array_f[0] <> 0 GROUP BY array_n[0], array_s[0], array_f[0] HAVING array_n[0] BETWEEN -1222 AND 21233 LIMIT 5 OFFSET 0;
--Testcase 190:
SELECT array_n[0], array_s[0], array_f[0] FROM array_test WHERE array_f[0] <> 0 GROUP BY array_n[0], array_s[0], array_f[0] HAVING array_n[0] BETWEEN -1222 AND 21233 LIMIT 5 OFFSET 0;

--Testcase 191:
EXPLAIN VERBOSE 
SELECT array_jn->'key1', array_js->'key1', array_jf->'key1', array_jf->'key2'->'key22'->0 FROM array_test WHERE (array_jn->'key2'->0)::int >= 0 GROUP BY array_jn, array_js, array_jf HAVING (array_jf->'key2'->'key22'->0)::float IN (65967.06531806084, -10797.319506069369, 80596.89133181405, 1.1);
--Testcase 192:
SELECT array_jn->'key1', array_js->'key1', array_jf->'key1', array_jf->'key2'->'key22'->0 FROM array_test WHERE (array_jn->'key2'->0)::int >= 0 GROUP BY array_jn, array_js, array_jf HAVING (array_jf->'key2'->'key22'->0)::float IN (65967.06531806084, -10797.319506069369, 80596.89133181405, 1.1);

-- test array extension
--Testcase 193:
EXPLAIN VERBOSE 
DELETE FROM array_test;
--Testcase 194:
DELETE FROM array_test;

--Testcase 195:
EXPLAIN VERBOSE 
INSERT INTO array_test VALUES(19, array[1,2,null,4], array['one','two',null,'four'], array[551.2121, 5125.5, 45.2, -45.36],'{"key1": "-0vkD1vkD1", "key2": [-82239, -27568]}', '{"key1": "^4jzR6jzR8.", "key2": ["}6tUr7tUr9<", " <4khS2khS9_", " #1HvE5HvE2", " #8qnh4qnh3<"]}', '{"key1": "_4TbU3TbU1@", "key2": {"key22": [65201.37933343978, -43867.89668332935, 89612.16240976125, 93294.21355107482]}}');
--Testcase 196:
INSERT INTO array_test VALUES(19, array[1,2,null,4], array['one','two',null,'four'], array[551.2121, 5125.5, 45.2, -45.36],'{"key1": "-0vkD1vkD1", "key2": [-82239, -27568]}', '{"key1": "^4jzR6jzR8.", "key2": ["}6tUr7tUr9<", " <4khS2khS9_", " #1HvE5HvE2", " #8qnh4qnh3<"]}', '{"key1": "_4TbU3TbU1@", "key2": {"key22": [65201.37933343978, -43867.89668332935, 89612.16240976125, 93294.21355107482]}}');

--Testcase 197:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 198:
SELECT * FROM array_test;

--Testcase 199:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[2] = 22, array_s[2] = 'array_swenarray_sy-array_swo';
--Testcase 200:
UPDATE array_test SET array_n[2] = 22, array_s[2] = 'array_swenarray_sy-array_swo';

--Testcase 201:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 202:
SELECT * FROM array_test;

--Testcase 203:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[5] = 5, array_s[5] = 'farray_nve';
--Testcase 204:
UPDATE array_test SET array_n[5] = 5, array_s[5] = 'farray_nve';

--Testcase 205:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 206:
SELECT * FROM array_test;

--Testcase 207:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[8] = 8, array_s[8] = 'earray_ngharray_s';
--Testcase 208:
UPDATE array_test SET array_n[8] = 8, array_s[8] = 'earray_ngharray_s';

--Testcase 209:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 210:
SELECT * FROM array_test;

--Testcase 211:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[0] = 0, array_s[0] = 'zero';
--Testcase 212:
UPDATE array_test SET array_n[0] = 0, array_s[0] = 'zero';

--Testcase 213:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 214:
SELECT * FROM array_test;

--Testcase 215:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[-3] = -3, array_s[-3] = 'marray_nnus-array_shree';
--Testcase 216:
UPDATE array_test SET array_n[-3] = -3, array_s[-3] = 'marray_nnus-array_shree';

--Testcase 217:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 218:
SELECT * FROM array_test;

--Testcase 219:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[0:2] = array[10,11,12], array_s[0:2] = array['array_sen','eleven','array_swelve'];
--Testcase 220:
UPDATE array_test SET array_n[0:2] = array[10,11,12], array_s[0:2] = array['array_sen','eleven','array_swelve'];

--Testcase 221:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 222:
SELECT * FROM array_test;

--Testcase 223:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[8:10] = array[18,null,20], array_s[8:10] = array['p18',null,'p20'];
--Testcase 224:
UPDATE array_test SET array_n[8:10] = array[18,null,20], array_s[8:10] = array['p18',null,'p20'];

--Testcase 225:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 226:
SELECT * FROM array_test;

--Testcase 227:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[11:12] = array[null,22], array_s[11:12] = array[null,'p22'];
--Testcase 228:
UPDATE array_test SET array_n[11:12] = array[null,22], array_s[11:12] = array[null,'p22'];

--Testcase 229:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 230:
SELECT * FROM array_test;

--Testcase 231:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[15:16] = array[null,26], array_s[15:16] = array[null,'p26'];
--Testcase 232:
UPDATE array_test SET array_n[15:16] = array[null,26], array_s[15:16] = array[null,'p26'];

--Testcase 233:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 234:
SELECT * FROM array_test;

--Testcase 235:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[-5:-3] = array[-15,-14,-13], array_s[-5:-3] = array['m15','m14','m13'];
--Testcase 236:
UPDATE array_test SET array_n[-5:-3] = array[-15,-14,-13], array_s[-5:-3] = array['m15','m14','m13'];

--Testcase 237:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 238:
SELECT * FROM array_test;

--Testcase 239:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[-7:-6] = array[-17,null], array_s[-7:-6] = array['m17',null];
--Testcase 240:
UPDATE array_test SET array_n[-7:-6] = array[-17,null], array_s[-7:-6] = array['m17',null];

--Testcase 241:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 242:
SELECT * FROM array_test;

--Testcase 243:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[-12:-10] = array[-22,null,-20], array_s[-12:-10] = array['m22',null,'m20'];
--Testcase 244:
UPDATE array_test SET array_n[-12:-10] = array[-22,null,-20], array_s[-12:-10] = array['m22',null,'m20'];

--Testcase 245:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 246:
SELECT * FROM array_test;

--Testcase 247:
EXPLAIN VERBOSE 
DELETE FROM array_test;
--Testcase 248:
DELETE FROM array_test;

--Testcase 249:
EXPLAIN VERBOSE 
INSERT INTO array_test VALUES(20, array[1,2,null,4], array['one','two',null,'four'], array[551.2121, 5125.5, 45.2, -45.36],'{"key1": "-0vkD1vkD1", "key2": [-82239, -27568]}', '{"key1": "^4jzR6jzR8.", "key2": ["}6tUr7tUr9<", " <4khS2khS9_", " #1HvE5HvE2", " #8qnh4qnh3<"]}', '{"key1": "_4TbU3TbU1@", "key2": {"key22": [65201.37933343978, -43867.89668332935, 89612.16240976125, 93294.21355107482]}}');
--Testcase 250:
INSERT INTO array_test VALUES(20, array[1,2,null,4], array['one','two',null,'four'], array[551.2121, 5125.5, 45.2, -45.36],'{"key1": "-0vkD1vkD1", "key2": [-82239, -27568]}', '{"key1": "^4jzR6jzR8.", "key2": ["}6tUr7tUr9<", " <4khS2khS9_", " #1HvE5HvE2", " #8qnh4qnh3<"]}', '{"key1": "_4TbU3TbU1@", "key2": {"key22": [65201.37933343978, -43867.89668332935, 89612.16240976125, 93294.21355107482]}}');

--Testcase 251:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 252:
SELECT * FROM array_test;

--Testcase 253:
EXPLAIN VERBOSE 
UPDATE array_test SET array_n[0:5] = array[0,1,2,null,4,5], array_s[0:5] = array['z','p1','p2',null,'p4','p5'];
--Testcase 254:
UPDATE array_test SET array_n[0:5] = array[0,1,2,null,4,5], array_s[0:5] = array['z','p1','p2',null,'p4','p5'];

--Testcase 255:
EXPLAIN VERBOSE 
SELECT * FROM array_test;
--Testcase 256:
SELECT * FROM array_test;


--Testcase 257:
DROP FOREIGN TABLE array_test;


-- TEST FOR table json mixed type

--Testcase 258:
CREATE FOREIGN TABLE mixed_types (
    id int, 
    score float, 
    numbers float[], 
    name text, 
    color int[], 
    active boolean, 
    active_lock_reason text, 
    created_at timestamp, 
    events text[], 
    description text, 
    teams jsonb)
SERVER dynamodb_server OPTIONS (table_name 'mixed_types', partition_key 'id');

--Testcase 259:
EXPLAIN VERBOSE 
SELECT * FROM mixed_types;
--Testcase 260:
SELECT * FROM mixed_types;

--Testcase 261:
EXPLAIN VERBOSE 
SELECT id, name, score, numbers, color FROM mixed_types;
--Testcase 262:
SELECT id, name, score, numbers, color FROM mixed_types;

--Testcase 263:
EXPLAIN VERBOSE 
SELECT id, name, description, active, active_lock_reason, teams FROM mixed_types;
--Testcase 264:
SELECT id, name, description, active, active_lock_reason, teams FROM mixed_types;

--Testcase 265:
EXPLAIN VERBOSE 
SELECT numbers, numbers[0], numbers[1], numbers[2] FROM mixed_types;
--Testcase 266:
SELECT numbers, numbers[0], numbers[1], numbers[2] FROM mixed_types;

--Testcase 267:
EXPLAIN VERBOSE 
SELECT color, events, color[0], events[0], events[3] FROM mixed_types;
--Testcase 268:
SELECT color, events, color[0], events[0], events[3] FROM mixed_types;

--Testcase 269:
EXPLAIN VERBOSE 
SELECT teams, teams->'name', teams->'parent'->'name', teams->'parent'->'id' FROM mixed_types;
--Testcase 270:
SELECT teams, teams->'name', teams->'parent'->'name', teams->'parent'->'id' FROM mixed_types;

--Testcase 271:
EXPLAIN VERBOSE 
SELECT teams, teams->>'name', teams->'parent'->>'name', teams->'parent'->>'id' FROM mixed_types;
--Testcase 272:
SELECT teams, teams->>'name', teams->'parent'->>'name', teams->'parent'->>'id' FROM mixed_types;

--Testcase 273:
EXPLAIN VERBOSE 
SELECT active AND true, active_lock_reason IS NULL FROM mixed_types; 
--Testcase 274:
SELECT active AND true, active_lock_reason IS NULL FROM mixed_types; 

--Testcase 275:
EXPLAIN VERBOSE 
SELECT id, name || description, active_lock_reason, color FROM mixed_types;
--Testcase 276:
SELECT id, name || description, active_lock_reason, color FROM mixed_types;

--Testcase 277:
EXPLAIN VERBOSE 
SELECT id + score, color[0]/numbers[0], color[1]-numbers[1], color[2]*numbers[2] FROM mixed_types;
--Testcase 278:
SELECT id + score, color[0]/numbers[0], color[1]-numbers[1], color[2]*numbers[2] FROM mixed_types;

--Testcase 279:
EXPLAIN VERBOSE 
SELECT * FROM mixed_types WHERE id <> 0;
--Testcase 280:
SELECT * FROM mixed_types WHERE id <> 0;

--Testcase 281:
EXPLAIN VERBOSE 
SELECT name, numbers, color FROM mixed_types WHERE id NOT IN (1212, 21212, 38239);
--Testcase 282:
SELECT name, numbers, color FROM mixed_types WHERE id NOT IN (1212, 21212, 38239);

--Testcase 283:
EXPLAIN VERBOSE 
SELECT teams, teams->'name', teams->'parent'->'name', teams->'parent'->'id' FROM mixed_types WHERE score IN (59.54, 45.97, 748.556);
--Testcase 284:
SELECT teams, teams->'name', teams->'parent'->'name', teams->'parent'->'id' FROM mixed_types WHERE score IN (59.54, 45.97, 748.556);

--Testcase 285:
EXPLAIN VERBOSE 
SELECT teams->>'name', teams->'parent'->>'name', teams->'parent'->>'id' FROM mixed_types WHERE score >= -1221.331;
--Testcase 286:
SELECT teams->>'name', teams->'parent'->>'name', teams->'parent'->>'id' FROM mixed_types WHERE score >= -1221.331;

--Testcase 287:
EXPLAIN VERBOSE 
SELECT active AND true, active OR FALSE, active_lock_reason IS NOT NULL FROM mixed_types WHERE score <= 2112.12;
--Testcase 288:
SELECT active AND true, active OR FALSE, active_lock_reason IS NOT NULL FROM mixed_types WHERE score <= 2112.12;

--Testcase 289:
EXPLAIN VERBOSE 
SELECT id + color[0], id - color[1], id * color[2], id/color[0] FROM mixed_types WHERE color[0] != 0;
--Testcase 290:
SELECT id + color[0], id - color[1], id * color[2], id/color[0] FROM mixed_types WHERE color[0] != 0;

--Testcase 291:
EXPLAIN VERBOSE 
SELECT numbers[0] + color[0], numbers[1] * color[1], numbers[2]/(color[2]+2) FROM mixed_types WHERE active_lock_reason IS NULL;
--Testcase 292:
SELECT numbers[0] + color[0], numbers[1] * color[1], numbers[2]/(color[2]+2) FROM mixed_types WHERE active_lock_reason IS NULL;

--Testcase 293:
EXPLAIN VERBOSE 
SELECT events[0] || events[1] || events[2], events[3] || events[4], score FROM mixed_types WHERE score > 0.5454;
--Testcase 294:
SELECT events[0] || events[1] || events[2], events[3] || events[4], score FROM mixed_types WHERE score > 0.5454;

--Testcase 295:
EXPLAIN VERBOSE 
SELECT id + (teams->'parent'->>'id')::bigint, name, description FROM mixed_types WHERE name ||  description != 'Q#J(@AWJE)';
--Testcase 296:
SELECT id + (teams->'parent'->>'id')::bigint, name, description FROM mixed_types WHERE name ||  description != 'Q#J(@AWJE)';

--Testcase 297:
EXPLAIN VERBOSE 
SELECT 1, '4aw0', color[0], events[0], events[3], events[4] FROM mixed_types WHERE events IS NOT NULL AND array_length(events, 1) > 4;
--Testcase 298:
SELECT 1, '4aw0', color[0], events[0], events[3], events[4] FROM mixed_types WHERE events IS NOT NULL AND array_length(events, 1) > 4;

--Testcase 299:
EXPLAIN VERBOSE 
SELECT name FROM (SELECT name FROM mixed_types WHERE id > 0 OR score < 0) tbl LIMIT 5 OFFSET 1;
--Testcase 300:
SELECT name FROM (SELECT name FROM mixed_types WHERE id > 0 OR score < 0) tbl LIMIT 5 OFFSET 1;

--Testcase 301:
EXPLAIN VERBOSE 
SELECT id FROM (SELECT id FROM mixed_types WHERE color[0] > 0) tbl LIMIT 5 OFFSET 0;
--Testcase 302:
SELECT id FROM (SELECT id FROM mixed_types WHERE color[0] > 0) tbl LIMIT 5 OFFSET 0;

--Testcase 303:
EXPLAIN VERBOSE 
SELECT score, id FROM (SELECT score, id FROM mixed_types WHERE color[0] > 0 AND numbers[0] > 0) tbl LIMIT ALL OFFSET 1;
--Testcase 304:
SELECT score, id FROM (SELECT score, id FROM mixed_types WHERE color[0] > 0 AND numbers[0] > 0) tbl LIMIT ALL OFFSET 1;

--Testcase 305:
EXPLAIN VERBOSE 
SELECT color FROM (SELECT color FROM mixed_types WHERE name > '@#!S') tbl LIMIT NULL OFFSET 1;
--Testcase 306:
SELECT color FROM (SELECT color FROM mixed_types WHERE name > '@#!S') tbl LIMIT NULL OFFSET 1;

--Testcase 307:
EXPLAIN VERBOSE 
SELECT events FROM (SELECT events FROM mixed_types WHERE id IN (3887, 2991, 2747, 895, 212)) tbl LIMIT 5 OFFSET 0;
--Testcase 308:
SELECT events FROM (SELECT events FROM mixed_types WHERE id IN (3887, 2991, 2747, 895, 212)) tbl LIMIT 5 OFFSET 0;

--Testcase 309:
EXPLAIN VERBOSE 
SELECT teams FROM (SELECT teams FROM mixed_types WHERE active_lock_reason IS NOT NULL) tbl LIMIT 3 OFFSET 0;
--Testcase 310:
SELECT teams FROM (SELECT teams FROM mixed_types WHERE active_lock_reason IS NOT NULL) tbl LIMIT 3 OFFSET 0;

--Testcase 311:
EXPLAIN VERBOSE 
SELECT numbers, teams FROM (SELECT numbers, teams FROM mixed_types WHERE active = true) tbl LIMIT 3 OFFSET 1;
--Testcase 312:
SELECT numbers, teams FROM (SELECT numbers, teams FROM mixed_types WHERE active = true) tbl LIMIT 3 OFFSET 1;

--Testcase 313:
EXPLAIN VERBOSE 
SELECT * FROM (SELECT * FROM mixed_types WHERE id > 0 AND color[0] > 0) tbl LIMIT 3 OFFSET 0;
--Testcase 314:
SELECT * FROM (SELECT * FROM mixed_types WHERE id > 0 AND color[0] > 0) tbl LIMIT 3 OFFSET 0;

--Testcase 315:
EXPLAIN VERBOSE 
SELECT 23, '2313', name FROM (SELECT * FROM mixed_types WHERE active = false) tbl LIMIT 3 OFFSET 0;
--Testcase 316:
SELECT 23, '2313', name FROM (SELECT * FROM mixed_types WHERE active = false) tbl LIMIT 3 OFFSET 0;

--Testcase 317:
EXPLAIN VERBOSE 
SELECT score + id, name || description FROM (SELECT score, id, name, description FROM mixed_types WHERE active_lock_reason IS NULL) tbl LIMIT 5 OFFSET 0;
--Testcase 318:
SELECT score + id, name || description FROM (SELECT score, id, name, description FROM mixed_types WHERE active_lock_reason IS NULL) tbl LIMIT 5 OFFSET 0;

--Testcase 319:
EXPLAIN VERBOSE 
SELECT id, name FROM mixed_types WHERE id > 0 GROUP BY id, name ORDER BY id, name LIMIT 5;
--Testcase 320:
SELECT id, name FROM mixed_types WHERE id > 0 GROUP BY id, name ORDER BY id, name LIMIT 5;

--Testcase 321:
EXPLAIN VERBOSE 
SELECT color[0]+color[1]*color[2], numbers[0]/numbers[1]+numbers[2], score FROM mixed_types WHERE numbers[1] <> 0 GROUP BY color, numbers, score, id ORDER BY id LIMIT ALL;
--Testcase 322:
SELECT color[0]+color[1]*color[2], numbers[0]/numbers[1]+numbers[2], score FROM mixed_types WHERE numbers[1] <> 0 GROUP BY color, numbers, score, id ORDER BY id LIMIT ALL;

--Testcase 323:
EXPLAIN VERBOSE 
SELECT (teams->'parent'->>'id')::bigint/numbers[0], teams->'parent'->>'name' FROM mixed_types WHERE numbers[0] > 0 GROUP BY teams, numbers ORDER BY teams->'parent'->>'name' LIMIT 3;
--Testcase 324:
SELECT (teams->'parent'->>'id')::bigint/numbers[0], teams->'parent'->>'name' FROM mixed_types WHERE numbers[0] > 0 GROUP BY teams, numbers ORDER BY teams->'parent'->>'name' LIMIT 3;

--Testcase 325:
EXPLAIN VERBOSE 
SELECT avg((teams->'parent'->>'id')::bigint), sum(color[0]), avg(numbers[1]) FROM mixed_types WHERE active_lock_reason IS NULL GROUP BY name ORDER BY name LIMIT 5;
--Testcase 326:
SELECT avg((teams->'parent'->>'id')::bigint), sum(color[0]), avg(numbers[1]) FROM mixed_types WHERE active_lock_reason IS NULL GROUP BY name ORDER BY name LIMIT 5;

--Testcase 327:
EXPLAIN VERBOSE 
SELECT id, name, description FROM mixed_types WHERE id IN (SELECT id FROM mixed_types WHERE active_lock_reason IS NULL) GROUP BY name, description, id ORDER BY name LIMIT 5;
--Testcase 328:
SELECT id, name, description FROM mixed_types WHERE id IN (SELECT id FROM mixed_types WHERE active_lock_reason IS NULL) GROUP BY name, description, id ORDER BY name LIMIT 5;

--Testcase 329:
EXPLAIN VERBOSE 
SELECT json_agg((name, '!@!*')), jsonb_agg((name, 'varr')), json_object_agg(id, 'x'), jsonb_object_agg(id, '23') FROM mixed_types WHERE description IS NOT NULL GROUP BY name, description ORDER BY description LIMIT 1;
--Testcase 330:
SELECT json_agg((name, '!@!*')), jsonb_agg((name, 'varr')), json_object_agg(id, 'x'), jsonb_object_agg(id, '23') FROM mixed_types WHERE description IS NOT NULL GROUP BY name, description ORDER BY description LIMIT 1;

--Testcase 331:
EXPLAIN VERBOSE 
SELECT active_lock_reason, active, score FROM mixed_types WHERE score <> ALL(SELECT id FROM mixed_types) GROUP BY active, active_lock_reason, score ORDER BY score LIMIT 5;
--Testcase 332:
SELECT active_lock_reason, active, score FROM mixed_types WHERE score <> ALL(SELECT id FROM mixed_types) GROUP BY active, active_lock_reason, score ORDER BY score LIMIT 5;

--Testcase 333:
EXPLAIN VERBOSE 
SELECT color[0], color[1], numbers[0], numbers[1] FROM mixed_types WHERE events[0] != 'DELETE' GROUP BY color[0], color[1], numbers[0], numbers[1] ORDER BY color[0], color[1], numbers[0], numbers[1] LIMIT 5;
--Testcase 334:
SELECT color[0], color[1], numbers[0], numbers[1] FROM mixed_types WHERE events[0] != 'DELETE' GROUP BY color[0], color[1], numbers[0], numbers[1] ORDER BY color[0], color[1], numbers[0], numbers[1] LIMIT 5;

--Testcase 335:
EXPLAIN VERBOSE 
SELECT numbers[0] + color[0], numbers[1] * color[1], numbers[2]/(color[2]+2) FROM mixed_types WHERE id > 0 GROUP BY id, color[0], color[1], color[2], numbers[0], numbers[1], numbers[2] ORDER BY id;
--Testcase 336:
SELECT numbers[0] + color[0], numbers[1] * color[1], numbers[2]/(color[2]+2) FROM mixed_types WHERE id > 0 GROUP BY id, color[0], color[1], color[2], numbers[0], numbers[1], numbers[2] ORDER BY id;

--Testcase 337:
EXPLAIN VERBOSE 
SELECT score, id FROM (SELECT score, id, active_lock_reason FROM mixed_types WHERE color[0] > 0 AND numbers[0] > 0) tbl WHERE tbl.active_lock_reason IS NULL GROUP BY score, id ORDER BY score, id;
--Testcase 338:
SELECT score, id FROM (SELECT score, id, active_lock_reason FROM mixed_types WHERE color[0] > 0 AND numbers[0] > 0) tbl WHERE tbl.active_lock_reason IS NULL GROUP BY score, id ORDER BY score, id;


--Testcase 339:
DROP FOREIGN TABLE mixed_types;


-- TEST FOR json nested attribute

--Testcase 340:
CREATE FOREIGN TABLE students (
    "ID" int, 
    name text, 
    friends jsonb)
SERVER dynamodb_server OPTIONS (table_name 'students', partition_key 'ID');


--Testcase 341:
EXPLAIN VERBOSE 
SELECT * FROM students;
--Testcase 342:
SELECT * FROM students;

--Testcase 343:
EXPLAIN VERBOSE 
SELECT "ID", name, friends->'class_info', friends->'class_info', friends->'class_info' FROM students;
--Testcase 344:
SELECT "ID", name, friends->'class_info', friends->'class_info', friends->'class_info' FROM students;

--Testcase 345:
EXPLAIN VERBOSE 
SELECT friends->'class_info', friends->'login', friends->'class_info', friends->'login', friends->'class_info', friends->'login' FROM students;
--Testcase 346:
SELECT friends->'class_info', friends->'login', friends->'class_info', friends->'login', friends->'class_info', friends->'login' FROM students;

--Testcase 347:
EXPLAIN VERBOSE 
SELECT friends->'class_info'->'ID', friends->'class_info'->'name', friends->'class_info'->'isAtDorm', friends->'class_info'->'score_rand' FROM students;
--Testcase 348:
SELECT friends->'class_info'->'ID', friends->'class_info'->'name', friends->'class_info'->'isAtDorm', friends->'class_info'->'score_rand' FROM students;

--Testcase 349:
EXPLAIN VERBOSE 
SELECT friends->'login'->'ID', friends->'login'->'age', friends->'login'->'isUpdated', friends->'login'->'last_score', friends->'login'->'lastsignin' FROM students;
--Testcase 350:
SELECT friends->'login'->'ID', friends->'login'->'age', friends->'login'->'isUpdated', friends->'login'->'last_score', friends->'login'->'lastsignin' FROM students;

--Testcase 351:
EXPLAIN VERBOSE 
SELECT friends->'login'->>'ID', friends->'login'->>'age', friends->'login'->>'isUpdated', friends->'login'->>'last_score', friends->'login'->>'lastsignin' FROM students;
--Testcase 352:
SELECT friends->'login'->>'ID', friends->'login'->>'age', friends->'login'->>'isUpdated', friends->'login'->>'last_score', friends->'login'->>'lastsignin' FROM students;

--Testcase 353:
EXPLAIN VERBOSE 
SELECT friends#>'{login,ID}', friends#>>'{2,login,age}', friends->'login'->>'isUpdated', friends->'login'->>'last_score', friends->'login'->>'lastsignin' FROM students;
--Testcase 354:
SELECT friends#>'{login,ID}', friends#>>'{2,login,age}', friends->'login'->>'isUpdated', friends->'login'->>'last_score', friends->'login'->>'lastsignin' FROM students;

--Testcase 355:
EXPLAIN VERBOSE 
SELECT 1, 'a4w23', '@)#malf', 232 FROM students;
--Testcase 356:
SELECT 1, 'a4w23', '@)#malf', 232 FROM students;

--Testcase 357:
EXPLAIN VERBOSE 
SELECT name || '::name', friends->'class_info'->>'name' || ' at dorm ', friends->'class_info'->>'isAtDorm' FROM students;
--Testcase 358:
SELECT name || '::name', friends->'class_info'->>'name' || ' at dorm ', friends->'class_info'->>'isAtDorm' FROM students;

--Testcase 359:
EXPLAIN VERBOSE 
SELECT "ID", name, (friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'ID')::int / (friends->'class_info'->>'score_rand')::float FROM students;
--Testcase 360:
SELECT "ID", name, (friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'ID')::int / (friends->'class_info'->>'score_rand')::float FROM students;

--Testcase 361:
EXPLAIN VERBOSE 
SELECT 33, '234asefAK@', (friends->'class_info'->>'score_rand')::float, (friends->'class_info'->>'score_rand')::float + 212.2 FROM students;
--Testcase 362:
SELECT 33, '234asefAK@', (friends->'class_info'->>'score_rand')::float, (friends->'class_info'->>'score_rand')::float + 212.2 FROM students;

--Testcase 363:
EXPLAIN VERBOSE 
SELECT * FROM students WHERE "ID" > 555;
--Testcase 364:
SELECT * FROM students WHERE "ID" > 555;

--Testcase 365:
EXPLAIN VERBOSE 
SELECT "ID", name, friends FROM students WHERE "ID" <> 5454;
--Testcase 366:
SELECT "ID", name, friends FROM students WHERE "ID" <> 5454;

--Testcase 367:
EXPLAIN VERBOSE 
SELECT name, friends->'class_info', friends->'class_info', friends->'class_info' FROM students WHERE friends->'class_info'->'ID' = friends->'login'->'ID';
--Testcase 368:
SELECT name, friends->'class_info', friends->'class_info', friends->'class_info' FROM students WHERE friends->'class_info'->'ID' = friends->'login'->'ID';

--Testcase 369:
EXPLAIN VERBOSE 
SELECT jsonb_extract_path(friends, 'login') FROM students WHERE friends->'login'->>'lastsignin' > '2015-07-10T12:06:09 -07:00';
--Testcase 370:
SELECT jsonb_extract_path(friends, 'login') FROM students WHERE friends->'login'->>'lastsignin' > '2015-07-10T12:06:09 -07:00';

--Testcase 371:
EXPLAIN VERBOSE 
SELECT name, "ID", friends, friends, friends FROM students WHERE "ID" != 456 AND name IS NOT NULL;
--Testcase 372:
SELECT name, "ID", friends, friends, friends FROM students WHERE "ID" != 456 AND name IS NOT NULL;

--Testcase 373:
EXPLAIN VERBOSE 
SELECT friends#>'{class_info,name}', friends#>'{login,ID}', friends#>'{login,lastsignin}' FROM students WHERE (friends->'login'->>'isUpdated')::boolean = false;
--Testcase 374:
SELECT friends#>'{class_info,name}', friends#>'{login,ID}', friends#>'{login,lastsignin}' FROM students WHERE (friends->'login'->>'isUpdated')::boolean = false;

--Testcase 375:
EXPLAIN VERBOSE 
SELECT friends->>0, friends->>1, friends->>2 FROM students WHERE (friends->'login'->>'last_score')::float > 0 OR (friends->'class_info'->>'score_rand')::float < 0;
--Testcase 376:
SELECT friends->>0, friends->>1, friends->>2 FROM students WHERE (friends->'login'->>'last_score')::float > 0 OR (friends->'class_info'->>'score_rand')::float < 0;

--Testcase 377:
EXPLAIN VERBOSE 
SELECT "ID", name, friends->'class_info'->'name', friends->'class_info'->'name', friends->'class_info'->'name' FROM students WHERE (friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float <> 2362.221;
--Testcase 378:
SELECT "ID", name, friends->'class_info'->'name', friends->'class_info'->'name', friends->'class_info'->'name' FROM students WHERE (friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float <> 2362.221;

--Testcase 379:
EXPLAIN VERBOSE 
SELECT sum((friends->'login'->>'last_score')::float), avg((friends->'class_info'->>'score_rand')::float), stddev((friends->'class_info'->>'score_rand')::float)/2 FROM students WHERE "ID" > 0;
--Testcase 380:
SELECT sum((friends->'login'->>'last_score')::float), avg((friends->'class_info'->>'score_rand')::float), stddev((friends->'class_info'->>'score_rand')::float)/2 FROM students WHERE "ID" > 0;

--Testcase 381:
EXPLAIN VERBOSE 
SELECT "ID", name FROM (SELECT (friends->'class_info'->>'ID')::int AS "ID", friends->'class_info'->>'name' AS name FROM students) tbl GROUP BY "ID", name HAVING "ID" IN (15455, 1969, 9895);
--Testcase 382:
SELECT "ID", name FROM (SELECT (friends->'class_info'->>'ID')::int AS "ID", friends->'class_info'->>'name' AS name FROM students) tbl GROUP BY "ID", name HAVING "ID" IN (15455, 1969, 9895);

--Testcase 383:
EXPLAIN VERBOSE 
SELECT name FROM (SELECT name FROM students WHERE name IN ('Angell', 'Olivia')) tbl GROUP BY name HAVING name NOT IN ('ALLY', 'CHESS');
--Testcase 384:
SELECT name FROM (SELECT name FROM students WHERE name IN ('Angell', 'Olivia')) tbl GROUP BY name HAVING name NOT IN ('ALLY', 'CHESS');

--Testcase 385:
EXPLAIN VERBOSE 
SELECT _name, _id FROM (SELECT friends->'class_info'->'name' AS _name, friends->'login'->'ID' as _id FROM students) tt GROUP BY _name, _id HAVING (_id)::int >= 0;
--Testcase 386:
SELECT _name, _id FROM (SELECT friends->'class_info'->'name' AS _name, friends->'login'->'ID' as _id FROM students) tt GROUP BY _name, _id HAVING (_id)::int >= 0;

--Testcase 387:
EXPLAIN VERBOSE 
SELECT name, tt FROM (SELECT friends->'class_info'->>'name' as name, friends->'login'->>'isUpdated' as tt FROM students) tbl GROUP BY tbl.name, tbl.tt HAVING (tt)::boolean != true;
--Testcase 388:
SELECT name, tt FROM (SELECT friends->'class_info'->>'name' as name, friends->'login'->>'isUpdated' as tt FROM students) tbl GROUP BY tbl.name, tbl.tt HAVING (tt)::boolean != true;
--Testcase 389:
EXPLAIN VERBOSE 
SELECT count(name), avg((friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float) FROM students WHERE EXISTS (SELECT "ID", name FROM students WHERE "ID" IN (15455, 1969, 9895)) GROUP BY name HAVING name IS NOT NULL;
--Testcase 390:
SELECT count(name), avg((friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float) FROM students WHERE EXISTS (SELECT "ID", name FROM students WHERE "ID" IN (15455, 1969, 9895)) GROUP BY name HAVING name IS NOT NULL;

--Testcase 391:
EXPLAIN VERBOSE 
SELECT name, (friends->'class_info'->>'ID')::int + (friends->'login'->>'ID')::int FROM students WHERE (friends->'class_info'->>'ID')::int IN (SELECT (friends->'class_info'->>'ID')::int FROM students) GROUP BY name, friends HAVING (friends->'class_info'->>'score_rand')::float > 0.01;
--Testcase 392:
SELECT name, (friends->'class_info'->>'ID')::int + (friends->'login'->>'ID')::int FROM students WHERE (friends->'class_info'->>'ID')::int IN (SELECT (friends->'class_info'->>'ID')::int FROM students) GROUP BY name, friends HAVING (friends->'class_info'->>'score_rand')::float > 0.01;

--Testcase 393:
EXPLAIN VERBOSE 
SELECT count(friends), count("ID"), min(friends->'class_info'->>'name'), max(friends->'class_info'->>'ID') FROM students WHERE "ID" >= ALL(SELECT (friends->'class_info'->'"ID"')::int FROM students) GROUP BY "ID" HAVING min(friends->'class_info'->>'score_rand') != min(friends->'login'->>'last_score');
--Testcase 394:
SELECT count(friends), count("ID"), min(friends->'class_info'->>'name'), max(friends->'class_info'->>'ID') FROM students WHERE "ID" >= ALL(SELECT (friends->'class_info'->'"ID"')::int FROM students) GROUP BY "ID" HAVING min(friends->'class_info'->>'score_rand') != min(friends->'login'->>'last_score');

--Testcase 395:
EXPLAIN VERBOSE 
SELECT sum((friends->'login'->>'ID')::int + (friends->'login'->>'ID')::int) + 3, count(name)/2 FROM students WHERE (friends->'login'->>'ID')::int = ANY(SELECT (friends->'login'->>'ID')::int FROM students) GROUP BY "ID" HAVING min((friends->'class_info'->>'score_rand')::float) <> 0;
--Testcase 396:
SELECT sum((friends->'login'->>'ID')::int + (friends->'login'->>'ID')::int) + 3, count(name)/2 FROM students WHERE (friends->'login'->>'ID')::int = ANY(SELECT (friends->'login'->>'ID')::int FROM students) GROUP BY "ID" HAVING min((friends->'class_info'->>'score_rand')::float) <> 0;

--Testcase 397:
EXPLAIN VERBOSE 
SELECT min(friends->'login'->>'ID'), min(friends->'login'->>'age'), count(DISTINCT friends->'login'->>'isUpdated') FROM students WHERE (friends->'login'->>'isUpdated')::boolean IN (SELECT (friends->'class_info'->>'isAtDorm')::boolean FROM students) GROUP BY "ID", name HAVING max((friends->'login'->>'age')::int) < 50;
--Testcase 398:
SELECT min(friends->'login'->>'ID'), min(friends->'login'->>'age'), count(DISTINCT friends->'login'->>'isUpdated') FROM students WHERE (friends->'login'->>'isUpdated')::boolean IN (SELECT (friends->'class_info'->>'isAtDorm')::boolean FROM students) GROUP BY "ID", name HAVING max((friends->'login'->>'age')::int) < 50;

--Testcase 399:
EXPLAIN VERBOSE 
SELECT name, "ID" FROM students WHERE "ID" IN (SELECT "ID" FROM students) GROUP BY name, "ID" HAVING name IS NOT NULL AND "ID" > 0;
--Testcase 400:
SELECT name, "ID" FROM students WHERE "ID" IN (SELECT "ID" FROM students) GROUP BY name, "ID" HAVING name IS NOT NULL AND "ID" > 0;

--Testcase 401:
EXPLAIN VERBOSE 
SELECT "ID", name FROM (SELECT (friends->'class_info'->>'ID')::int as "ID", friends->'class_info'->>'name' AS name FROM students) tbl GROUP BY "ID", name HAVING "ID" IN (15455, 1969, 9895) LIMIT 3;
--Testcase 402:
SELECT "ID", name FROM (SELECT (friends->'class_info'->>'ID')::int as "ID", friends->'class_info'->>'name' AS name FROM students) tbl GROUP BY "ID", name HAVING "ID" IN (15455, 1969, 9895) LIMIT 3;

--Testcase 403:
EXPLAIN VERBOSE 
SELECT name, is_update FROM (SELECT friends->'class_info'->>'name' AS name, friends->'login'->>'isUpdated' AS is_update FROM students) tbl GROUP BY name, is_update HAVING (is_update)::boolean != true LIMIT 5;
--Testcase 404:
SELECT name, is_update FROM (SELECT friends->'class_info'->>'name' AS name, friends->'login'->>'isUpdated' AS is_update FROM students) tbl GROUP BY name, is_update HAVING (is_update)::boolean != true LIMIT 5;

--Testcase 405:
EXPLAIN VERBOSE 
SELECT count(name), avg((friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float) FROM students WHERE EXISTS (SELECT "ID", name FROM students WHERE "ID" IN (15455, 1969, 9895)) GROUP BY name HAVING name IS NOT NULL LIMIT 1;
--Testcase 406:
SELECT count(name), avg((friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float) FROM students WHERE EXISTS (SELECT "ID", name FROM students WHERE "ID" IN (15455, 1969, 9895)) GROUP BY name HAVING name IS NOT NULL LIMIT 1;

--Testcase 407:
EXPLAIN VERBOSE 
SELECT friends->'login'->>'lastsignin', friends->'login'->>'lastsignin', friends->'login'->>'lastsignin' FROM students WHERE friends->'login'->>'lastsignin' < ALL(SELECT friends->'login'->>'lastsignin' FROM students WHERE (friends->'login'->>'last_score')::float <> 0) GROUP BY friends->'login'->>'lastsignin', friends->'login'->>'lastsignin', friends->'login'->>'lastsignin' HAVING min("ID") > 0 LIMIT 3;
--Testcase 408:
SELECT friends->'login'->>'lastsignin', friends->'login'->>'lastsignin', friends->'login'->>'lastsignin' FROM students WHERE friends->'login'->>'lastsignin' < ALL(SELECT friends->'login'->>'lastsignin' FROM students WHERE (friends->'login'->>'last_score')::float <> 0) GROUP BY friends->'login'->>'lastsignin', friends->'login'->>'lastsignin', friends->'login'->>'lastsignin' HAVING min("ID") > 0 LIMIT 3;

--Testcase 409:
EXPLAIN VERBOSE 
SELECT count(name), avg((friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float) FROM students WHERE EXISTS (SELECT "ID", name FROM students WHERE "ID" IN (15455, 1969, 9895)) GROUP BY name HAVING name IS NOT NULL LIMIT 3;
--Testcase 410:
SELECT count(name), avg((friends->'class_info'->>'score_rand')::float + (friends->'class_info'->>'score_rand')::float) FROM students WHERE EXISTS (SELECT "ID", name FROM students WHERE "ID" IN (15455, 1969, 9895)) GROUP BY name HAVING name IS NOT NULL LIMIT 3;

--Testcase 411:
EXPLAIN VERBOSE 
SELECT name, (friends->'class_info'->>'ID')::int + (friends->'login'->>'ID')::int FROM students WHERE (friends->'class_info'->>'ID')::int IN (SELECT (friends->'class_info'->>'ID')::int FROM students) GROUP BY name, (friends->'class_info'->>'ID')::int + (friends->'login'->>'ID')::int, (friends->'class_info'->>'score_rand')::float HAVING (friends->'class_info'->>'score_rand')::float > 0.01 LIMIT 3;
--Testcase 412:
SELECT name, (friends->'class_info'->>'ID')::int + (friends->'login'->>'ID')::int FROM students WHERE (friends->'class_info'->>'ID')::int IN (SELECT (friends->'class_info'->>'ID')::int FROM students) GROUP BY name, (friends->'class_info'->>'ID')::int + (friends->'login'->>'ID')::int, (friends->'class_info'->>'score_rand')::float HAVING (friends->'class_info'->>'score_rand')::float > 0.01 LIMIT 3;

--Testcase 413:
EXPLAIN VERBOSE 
SELECT count(friends), count(name), min(friends->'class_info'->>'name'), max(friends->'class_info'->>'ID') FROM students WHERE "ID" >= ALL(SELECT (friends->'class_info'->'ID')::int FROM students) GROUP BY "ID" HAVING min(friends->'class_info'->>'score_rand') != min(friends->'login'->>'last_score') LIMIT 3;
--Testcase 414:
SELECT count(friends), count(name), min(friends->'class_info'->>'name'), max(friends->'class_info'->>'ID') FROM students WHERE "ID" >= ALL(SELECT (friends->'class_info'->'ID')::int FROM students) GROUP BY "ID" HAVING min(friends->'class_info'->>'score_rand') != min(friends->'login'->>'last_score') LIMIT 3;

--Testcase 415:
EXPLAIN VERBOSE 
SELECT sum((friends->'login'->>'ID')::int) + (friends->'login'->>'ID')::int + 3, count(name)/2 FROM students WHERE (friends->'login'->>'ID')::int = ANY(SELECT (friends->'login'->>'ID')::int FROM students) GROUP BY "ID", friends HAVING min((friends->'class_info'->>'score_rand')::float) <> 0 LIMIT 1;
--Testcase 416:
SELECT sum((friends->'login'->>'ID')::int) + (friends->'login'->>'ID')::int + 3, count(name)/2 FROM students WHERE (friends->'login'->>'ID')::int = ANY(SELECT (friends->'login'->>'ID')::int FROM students) GROUP BY "ID", friends HAVING min((friends->'class_info'->>'score_rand')::float) <> 0 LIMIT 1;

--Testcase 417:
EXPLAIN VERBOSE 
SELECT min(friends->'login'->>'ID'), min(friends->'login'->>'age'), count(DISTINCT friends->'login'->>'isUpdated') FROM students WHERE (friends->'login'->>'isUpdated')::boolean IN (SELECT (friends->'class_info'->>'isAtDorm')::boolean FROM students) GROUP BY "ID", name HAVING max((friends->'login'->>'age')::int) < 50 LIMIT 1;
--Testcase 418:
SELECT min(friends->'login'->>'ID'), min(friends->'login'->>'age'), count(DISTINCT friends->'login'->>'isUpdated') FROM students WHERE (friends->'login'->>'isUpdated')::boolean IN (SELECT (friends->'class_info'->>'isAtDorm')::boolean FROM students) GROUP BY "ID", name HAVING max((friends->'login'->>'age')::int) < 50 LIMIT 1;

--Testcase 419:
EXPLAIN VERBOSE 
SELECT name, "ID" FROM students WHERE "ID" IN (SELECT "ID" FROM students) GROUP BY name, "ID" HAVING name IS NOT NULL OR "ID" > 0 LIMIT 3;
--Testcase 420:
SELECT name, "ID" FROM students WHERE "ID" IN (SELECT "ID" FROM students) GROUP BY name, "ID" HAVING name IS NOT NULL OR "ID" > 0 LIMIT 3;


--Testcase 421:
DROP FOREIGN TABLE students;


-- TEST FOR others: operator, constant, orderby/groupby/limit

--Testcase 422:
CREATE FOREIGN TABLE classes (
    id int, 
    name text, 
    "isAtDorm" boolean, 
    score float, 
    courses jsonb) 
SERVER dynamodb_server OPTIONS (table_name 'classes', partition_key 'id');

--Testcase 423:
CREATE FOREIGN TABLE J1_TBL (
    "ID" int, 
    q1 int, 
    q2 float8, 
    q3 text)
SERVER dynamodb_server OPTIONS (table_name 'J1_TBL', partition_key 'ID');

--Testcase 424:
CREATE FOREIGN TABLE J2_TBL (
    "ID" int, 
    q1 int, 
    q2 float8) 
SERVER dynamodb_server OPTIONS (table_name 'J2_TBL', partition_key 'ID');


--select constants
--Testcase 425:
EXPLAIN VERBOSE 
SELECT 1, 31, '1212e' FROM classes;
--Testcase 426:
SELECT 1, 31, '1212e' FROM classes;

--Testcase 427:
EXPLAIN VERBOSE 
SELECT 1,'XYZ' union all SELECT 2,'abc' FROM classes;
--Testcase 428:
SELECT 1,'XYZ' union all SELECT 2,'abc' FROM classes;

--Testcase 429:
EXPLAIN VERBOSE 
SELECT * FROM (SELECT 1, 2, 3, 4 FROM classes) AS t(a,b,c,d);
--Testcase 430:
SELECT * FROM (SELECT 1, 2, 3, 4 FROM classes) AS t(a,b,c,d);

--Testcase 431:
EXPLAIN VERBOSE 
SELECT constant1, constant2, constant3 FROM (SELECT 'foo@gmail.com', 'bar@gmail.com', 'baz@gmail.com' FROM classes) AS mytable(constant1, constant2, constant3);
--Testcase 432:
SELECT constant1, constant2, constant3 FROM (SELECT 'foo@gmail.com', 'bar@gmail.com', 'baz@gmail.com' FROM classes) AS mytable(constant1, constant2, constant3);

--Testcase 433:
EXPLAIN VERBOSE 
SELECT * FROM (VALUES (1,'XYZ'),(2,'abc'))  AS t (id, data);
--Testcase 434:
SELECT * FROM (VALUES (1,'XYZ'),(2,'abc'))  AS t (id, data);

--Testcase 435:
EXPLAIN VERBOSE 
SELECT 1212, 'AM(!WF)', 12 FROM classes;
--Testcase 436:
SELECT 1212, 'AM(!WF)', 12 FROM classes;

--Testcase 437:
EXPLAIN VERBOSE 
SELECT 1212, 'AM(!WF)', 12 FROM classes WHERE id > 0 GROUP BY id HAVING id NOT IN (11, 233, 331) ORDER BY id LIMIT 1 OFFSET 1;
--Testcase 438:
SELECT 1212, 'AM(!WF)', 12 FROM classes WHERE id > 0 GROUP BY id HAVING id NOT IN (11, 233, 331) ORDER BY id LIMIT 1 OFFSET 1;

--insert/update/delete
--Testcase 439:
EXPLAIN VERBOSE 
INSERT INTO classes VALUES (25, 'Halla Hue', true, 3.26, '{"majors": "IT", "sub-majors": "Networking"}');
--Testcase 440:
INSERT INTO classes VALUES (25, 'Halla Hue', true, 3.26, '{"majors": "IT", "sub-majors": "Networking"}');

--Testcase 441:
EXPLAIN VERBOSE 
INSERT INTO classes (id, name, courses) VALUES (26, 'Anna Hat', '{"majors": "Astronomy", "sub-majors": ""}');
--Testcase 442:
INSERT INTO classes (id, name, courses) VALUES (26, 'Anna Hat', '{"majors": "Astronomy", "sub-majors": ""}');

--Testcase 443:
EXPLAIN VERBOSE 
INSERT INTO classes (id, courses) VALUES (27, null);
--Testcase 444:
INSERT INTO classes (id, courses) VALUES (27, null);

--Testcase 445:
EXPLAIN VERBOSE 
INSERT INTO classes (id, name, score, courses) VALUES (28, 'Welling Jr', 23.12, '{"majors": null, "sub-majors": "Chemistry"}');
--Testcase 446:
INSERT INTO classes (id, name, score, courses) VALUES (28, 'Welling Jr', 23.12, '{"majors": null, "sub-majors": "Chemistry"}');

--Testcase 447:
EXPLAIN VERBOSE 
INSERT INTO classes VALUES (29, 'HASHKEYS KEEP', false, 23.12);
--Testcase 448:
INSERT INTO classes VALUES (29, 'HASHKEYS KEEP', false, 23.12);

--Testcase 449:
EXPLAIN VERBOSE 
INSERT INTO classes VALUES (30, 'Zone Diana', true, 45.2, '{"majors": "Archeology", "sub-majors": "Adventure"}');
--Testcase 450:
INSERT INTO classes VALUES (30, 'Zone Diana', true, 45.2, '{"majors": "Archeology", "sub-majors": "Adventure"}');

--Testcase 451:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = 28;
--Testcase 452:
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = 28;

--Testcase 453:
-- Should faild
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = -1;
--Testcase 454:
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = -1;

--Testcase 457:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{"majors": {"m1": "No1", "m2": "No2"}}' WHERE id = 27;
--Testcase 458:
UPDATE classes SET courses = '{"majors": {"m1": "No1", "m2": "No2"}}' WHERE id = 27;

--Testcase 459:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{"key1": "|2FSo3FSo4*", "key2":[70356, 24708, 68429, -84248, -83370]}' WHERE id = 25;
--Testcase 460:
UPDATE classes SET courses = '{"key1": "|2FSo3FSo4*", "key2":[70356, 24708, 68429, -84248, -83370]}' WHERE id = 25;

--Testcase 461:
EXPLAIN VERBOSE 
DELETE FROM classes WHERE id = 26;
--Testcase 462:
DELETE FROM classes WHERE id = 26;

--Testcase 463:
EXPLAIN VERBOSE 
DELETE FROM classes WHERE courses->'sub-majors' IS NULL AND id BETWEEN 25 AND 30;
--Testcase 464:
DELETE FROM classes WHERE courses->'sub-majors' IS NULL AND id BETWEEN 25 AND 30;

--Testcase 465:
EXPLAIN VERBOSE 
INSERT INTO classes (id, name, courses) VALUES (26, 'Anna Hat', '{"majors": "Astronomy", "sub-majors": ""}');
--Testcase 466:
INSERT INTO classes (id, name, courses) VALUES (26, 'Anna Hat', '{"majors": "Astronomy", "sub-majors": ""}');

--Testcase 467:
EXPLAIN VERBOSE 
UPDATE classes SET courses = null WHERE id = 26;
--Testcase 468:
UPDATE classes SET courses = null WHERE id = 26;

--Testcase 469:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '""' WHERE id = 25;
--Testcase 470:
UPDATE classes SET courses = '""' WHERE id = 25;

--Testcase 471:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '[]' WHERE id = 27;
--Testcase 472:
UPDATE classes SET courses = '[]' WHERE id = 27;

--Testcase 473:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{}' WHERE id = 29;
--Testcase 474:
UPDATE classes SET courses = '{}' WHERE id = 29;


--Operators (comparasion, logic, dereference)
--Testcase 525:
EXPLAIN VERBOSE 
SELECT * FROM classes WHERE id > 10;
--Testcase 526:
SELECT * FROM classes WHERE id > 10;

--Testcase 527:
EXPLAIN VERBOSE 
SELECT id, name, "isAtDorm" FROM classes WHERE id < 10 GROUP BY id, name, "isAtDorm" HAVING id <> 10 ORDER BY id; 
--Testcase 528:
SELECT id, name, "isAtDorm" FROM classes WHERE id < 10 GROUP BY id, name, "isAtDorm" HAVING id <> 10 ORDER BY id; 

--Testcase 529:
EXPLAIN VERBOSE 
SELECT name, score, courses->'majors', courses->'sub-majors' FROM classes WHERE courses->>'majors' = 'Math' AND courses->>'sub-majors' != 'Math';
--Testcase 530:
SELECT name, score, courses->'majors', courses->'sub-majors' FROM classes WHERE courses->>'majors' = 'Math' AND courses->>'sub-majors' != 'Math';

--Testcase 531:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM classes WHERE score >= 0;
--Testcase 532:
SELECT name, id + score, score FROM classes WHERE score >= 0;

--Testcase 533:
EXPLAIN VERBOSE 
SELECT name, score FROM classes GROUP BY name, score, courses HAVING courses->>'majors' = 'Math' ORDER BY name, score;
--Testcase 534:
SELECT name, score FROM classes GROUP BY name, score, courses HAVING courses->>'majors' = 'Math' ORDER BY name, score;

--Testcase 535:
EXPLAIN VERBOSE 
SELECT "isAtDorm" AND true, "isAtDorm" OR FALSE, id <> 10 OR true FROM classes;
--Testcase 536:
SELECT "isAtDorm" AND true, "isAtDorm" OR FALSE, id <> 10 OR true FROM classes;

--Testcase 537:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM classes WHERE score NOT IN (1212.3, 13.13, 424.3);
--Testcase 538:
SELECT name, score, courses FROM classes WHERE score NOT IN (1212.3, 13.13, 424.3);

--Testcase 539:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM classes WHERE id IN (1, 3, 4, 9, 20);
--Testcase 540:
SELECT name, score, courses FROM classes WHERE id IN (1, 3, 4, 9, 20);

--Testcase 541:
EXPLAIN VERBOSE 
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes WHERE id BETWEEN 1 AND 10;
--Testcase 542:
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes WHERE id BETWEEN 1 AND 10;

--Testcase 543:
EXPLAIN VERBOSE 
SELECT name, courses->>'majors', "isAtDorm", score FROM classes WHERE name != '@#!#' OR id >= 10 AND courses->>'majors' = 'Biology';
--Testcase 544:
SELECT name, courses->>'majors', "isAtDorm", score FROM classes WHERE name != '@#!#' OR id >= 10 AND courses->>'majors' = 'Biology';

--ORDER BY, GROUP BY, LIMIT
--ORDER BY

--Testcase 545:
EXPLAIN VERBOSE 
SELECT * FROM classes ORDER BY id, name;
--Testcase 546:
SELECT * FROM classes ORDER BY id, name;

--Testcase 547:
EXPLAIN VERBOSE 
SELECT id, name, courses FROM classes ORDER BY name;
--Testcase 548:
SELECT id, name, courses FROM classes ORDER BY name;

--Testcase 549:
EXPLAIN VERBOSE 
SELECT name, "isAtDorm", score FROM classes ORDER BY name ASC, "isAtDorm", score DESC;
--Testcase 550:
SELECT name, "isAtDorm", score FROM classes ORDER BY name ASC, "isAtDorm", score DESC;

--Testcase 551:
EXPLAIN VERBOSE 
SELECT id, name, score FROM classes ORDER BY id DESC;
--Testcase 552:
SELECT id, name, score FROM classes ORDER BY id DESC;

--Testcase 553:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes ORDER BY courses->>'majors';
--Testcase 554:
SELECT courses->'majors', courses->'sub-majors' FROM classes ORDER BY courses->>'majors';

--Testcase 555:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes ORDER BY courses->>'sub-majors';
--Testcase 556:
SELECT courses->'majors', courses->'sub-majors' FROM classes ORDER BY courses->>'sub-majors';

--Testcase 557:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM classes ORDER BY score;
--Testcase 558:
SELECT name, score, courses FROM classes ORDER BY score;

--Testcase 559:
EXPLAIN VERBOSE 
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes ORDER BY courses->>'majors', courses->>'sub-majors';
--Testcase 560:
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes ORDER BY courses->>'majors', courses->>'sub-majors';

--Testcase 561:
EXPLAIN VERBOSE 
SELECT name || ' ', courses->>'majors', "isAtDorm", score FROM classes ORDER BY score;
--Testcase 562:
SELECT name || ' ', courses->>'majors', "isAtDorm", score FROM classes ORDER BY score;

--Testcase 563:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM classes ORDER BY (id + score);
--Testcase 564:
SELECT name, id + score, score FROM classes ORDER BY (id + score);

--ORDER BY, WHERE
--Testcase 565:
EXPLAIN VERBOSE 
SELECT * FROM (SELECT * FROM classes WHERE id > 1) tbl WHERE id < 5 ORDER BY id, name;
--Testcase 566:
SELECT * FROM (SELECT * FROM classes WHERE id > 1) tbl WHERE id < 5 ORDER BY id, name;

--Testcase 567:
EXPLAIN VERBOSE 
SELECT id, name, courses FROM (SELECT * FROM classes WHERE id IN (1, 3, 4, 5, 9, 10)) tbl WHERE name <> 'Lora Hamilton' ORDER BY name;
--Testcase 568:
SELECT id, name, courses FROM (SELECT * FROM classes WHERE id IN (1, 3, 4, 5, 9, 10)) tbl WHERE name <> 'Lora Hamilton' ORDER BY name;

--Testcase 569:
EXPLAIN VERBOSE 
SELECT name, "isAtDorm", score FROM (SELECT name, "isAtDorm", score FROM classes WHERE id <> 5) tbl WHERE score < 0 ORDER BY name ASC, "isAtDorm", score DESC;
--Testcase 570:
SELECT name, "isAtDorm", score FROM (SELECT name, "isAtDorm", score FROM classes WHERE id <> 5) tbl WHERE score < 0 ORDER BY name ASC, "isAtDorm", score DESC;

--Testcase 571:
EXPLAIN VERBOSE 
SELECT id, name, score FROM (SELECT id, name, score FROM classes WHERE id >= 10) tbl WHERE score >= 0 ORDER BY id DESC;
--Testcase 572:
SELECT id, name, score FROM (SELECT id, name, score FROM classes WHERE id >= 10) tbl WHERE score >= 0 ORDER BY id DESC;

--Testcase 573:
EXPLAIN VERBOSE 
SELECT major, submajor FROM (SELECT courses->'majors' AS major, courses->'sub-majors' AS submajor FROM classes WHERE id <= 10) tbl WHERE (tbl.major)::text = 'Math' ORDER BY submajor;
--Testcase 574:
SELECT major, submajor FROM (SELECT courses->'majors' AS major, courses->'sub-majors' AS submajor FROM classes WHERE id <= 10) tbl WHERE (tbl.major)::text = 'Math' ORDER BY submajor;

--Testcase 575:
EXPLAIN VERBOSE 
SELECT major, submajor FROM (SELECT courses->>'majors' AS major, courses->>'sub-majors' AS submajor FROM classes WHERE id BETWEEN 10 AND 15) tbl WHERE tbl.major != 'Math' ORDER BY major;
--Testcase 576:
SELECT major, submajor FROM (SELECT courses->>'majors' AS major, courses->>'sub-majors' AS submajor FROM classes WHERE id BETWEEN 10 AND 15) tbl WHERE tbl.major != 'Math' ORDER BY major;

--Testcase 577:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM (SELECT name, courses, score FROM classes WHERE score > 0 AND score < 6564.15) tbl ORDER BY score;
--Testcase 578:
SELECT name, score, courses FROM (SELECT name, courses, score FROM classes WHERE score > 0 AND score < 6564.15) tbl ORDER BY score;

--Testcase 579:
EXPLAIN VERBOSE 
SELECT major, submajor, name FROM (SELECT courses->>'majors' AS major, courses->>'sub-majors' AS submajor, name, score FROM classes WHERE id > 5 AND id < 15) tbl WHERE score > 0 ORDER BY major, submajor;
--Testcase 580:
SELECT major, submajor, name FROM (SELECT courses->>'majors' AS major, courses->>'sub-majors' AS submajor, name, score FROM classes WHERE id > 5 AND id < 15) tbl WHERE score > 0 ORDER BY major, submajor;

--Testcase 581:
EXPLAIN VERBOSE 
SELECT name || ' ', major, "isAtDorm", score FROM (SELECT name, courses->>'majors' AS major, "isAtDorm", score FROM classes WHERE id NOT IN (1, 5, 6, 9, 7, 8))  tbl WHERE score > 0 ORDER BY score;
--Testcase 582:
SELECT name || ' ', major, "isAtDorm", score FROM (SELECT name, courses->>'majors' AS major, "isAtDorm", score FROM classes WHERE id NOT IN (1, 5, 6, 9, 7, 8))  tbl WHERE score > 0 ORDER BY score;

--Testcase 583:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM (SELECT name, id, score FROM classes WHERE score > 0 AND score < 6564.15) tbl WHERE id NOT IN (1, 6, 9, 8) ORDER BY (id + score);
--Testcase 584:
SELECT name, id + score, score FROM (SELECT name, id, score FROM classes WHERE score > 0 AND score < 6564.15) tbl WHERE id NOT IN (1, 6, 9, 8) ORDER BY (id + score);

--GROUP BY, ORDER BY
--Testcase 585:
EXPLAIN VERBOSE 
SELECT * FROM classes WHERE id < 5 GROUP BY id, name, courses, "isAtDorm", score ORDER BY id, name;
--Testcase 586:
SELECT * FROM classes WHERE id < 5 GROUP BY id, name, courses, "isAtDorm", score ORDER BY id, name;

--Testcase 587:
EXPLAIN VERBOSE 
SELECT id, name, courses FROM classes WHERE name <> 'Lora Hamilton' GROUP BY name, id, courses ORDER BY name;
--Testcase 588:
SELECT id, name, courses FROM classes WHERE name <> 'Lora Hamilton' GROUP BY name, id, courses ORDER BY name;

--Testcase 589:
EXPLAIN VERBOSE 
SELECT name, "isAtDorm", score FROM classes WHERE score >= 0 GROUP BY name, "isAtDorm", score ORDER BY name ASC, "isAtDorm", score DESC;
--Testcase 590:
SELECT name, "isAtDorm", score FROM classes WHERE score >= 0 GROUP BY name, "isAtDorm", score ORDER BY name ASC, "isAtDorm", score DESC;

--Testcase 591:
EXPLAIN VERBOSE 
SELECT id, name, score FROM classes GROUP BY id, name, score ORDER BY id DESC;
--Testcase 592:
SELECT id, name, score FROM classes GROUP BY id, name, score ORDER BY id DESC;

--Testcase 593:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id <= 10 GROUP BY courses, courses->'majors', courses->'sub-majors' ORDER BY courses->>'majors';
--Testcase 594:
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id <= 10 GROUP BY courses, courses->'majors', courses->'sub-majors' ORDER BY courses->>'majors';

--Testcase 595:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id BETWEEN 10 AND 15 GROUP BY courses, courses->'majors', courses->'sub-majors' ORDER BY courses->>'sub-majors';
--Testcase 596:
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id BETWEEN 10 AND 15 GROUP BY courses, courses->'majors', courses->'sub-majors' ORDER BY courses->>'sub-majors';

--Testcase 597:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM classes WHERE id > 5 AND id < 15 GROUP BY name, score, courses ORDER BY score;
--Testcase 598:
SELECT name, score, courses FROM classes WHERE id > 5 AND id < 15 GROUP BY name, score, courses ORDER BY score;

--Testcase 599:
EXPLAIN VERBOSE 
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes WHERE id NOT IN (1, 6, 9, 8) GROUP BY courses, name ORDER BY courses->>'majors', courses->>'sub-majors';
--Testcase 600:
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes WHERE id NOT IN (1, 6, 9, 8) GROUP BY courses, name ORDER BY courses->>'majors', courses->>'sub-majors';

--Testcase 601:
EXPLAIN VERBOSE 
SELECT name, courses->>'majors', "isAtDorm", score FROM classes WHERE score > 0 AND score < 6564.15 GROUP BY name, courses, "isAtDorm", score ORDER BY score;
--Testcase 602:
SELECT name, courses->>'majors', "isAtDorm", score FROM classes WHERE score > 0 AND score < 6564.15 GROUP BY name, courses, "isAtDorm", score ORDER BY score;

--Testcase 603:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM classes WHERE score >= 0 GROUP BY name, id, score ORDER BY (id + score);
--Testcase 604:
SELECT name, id + score, score FROM classes WHERE score >= 0 GROUP BY name, id, score ORDER BY (id + score);
--GROUP BY

--Testcase 605:
EXPLAIN VERBOSE 
SELECT * FROM (SELECT id, name FROM classes WHERE id > 10) tbl GROUP BY id, name;
--Testcase 606:
SELECT * FROM (SELECT id, name FROM classes WHERE id > 10) tbl GROUP BY id, name;

--Testcase 607:
EXPLAIN VERBOSE 
SELECT id, name, courses FROM (SELECT id, name, courses FROM classes WHERE id IN (1, 3, 4, 5, 8, 9)) tbl GROUP BY id, name, courses ORDER BY id, name, courses;
--Testcase 608:
SELECT id, name, courses FROM (SELECT id, name, courses FROM classes WHERE id IN (1, 3, 4, 5, 8, 9)) tbl GROUP BY id, name, courses ORDER BY id, name, courses;

--Testcase 609:
EXPLAIN VERBOSE 
SELECT name, "isAtDorm", score FROM (SELECT name, "isAtDorm", score FROM classes WHERE score > 0) tbl GROUP BY name, "isAtDorm", score;
--Testcase 610:
SELECT name, "isAtDorm", score FROM (SELECT name, "isAtDorm", score FROM classes WHERE score > 0) tbl GROUP BY name, "isAtDorm", score;

--Testcase 611:
EXPLAIN VERBOSE 
SELECT id, name, score FROM (SELECT id, name, score FROM classes WHERE name <= 'Sonja Reid') tbl GROUP BY id, name, score;
--Testcase 612:
SELECT id, name, score FROM (SELECT id, name, score FROM classes WHERE name <= 'Sonja Reid') tbl GROUP BY id, name, score;

--Testcase 613:
EXPLAIN VERBOSE 
SELECT _major, _sub FROM (SELECT courses->'majors' AS _major, courses->'sub-majors' AS _sub FROM classes WHERE courses->>'majors' = 'Math') tbl GROUP BY _major, _sub;
--Testcase 614:
SELECT _major, _sub FROM (SELECT courses->'majors' AS _major, courses->'sub-majors' AS _sub FROM classes WHERE courses->>'majors' = 'Math') tbl GROUP BY _major, _sub;

--Testcase 615:
EXPLAIN VERBOSE 
SELECT _major_, _sub_major_ FROM (SELECT courses->'majors' AS _major_, courses->'sub-majors' AS _sub_major_ FROM classes WHERE  courses->>'majors' != 'Math') tbl GROUP BY _major_, _sub_major_;
--Testcase 616:
SELECT _major_, _sub_major_ FROM (SELECT courses->'majors' AS _major_, courses->'sub-majors' AS _sub_major_ FROM classes WHERE  courses->>'majors' != 'Math') tbl GROUP BY _major_, _sub_major_;

--Testcase 617:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM (SELECT name, score, courses FROM classes WHERE score < 0) tbl GROUP BY name, score, courses;
--Testcase 618:
SELECT name, score, courses FROM (SELECT name, score, courses FROM classes WHERE score < 0) tbl GROUP BY name, score, courses;

--Testcase 619:
EXPLAIN VERBOSE 
SELECT majors, submajors, name FROM (SELECT courses->>'majors' AS majors, courses->>'sub-majors' AS submajors, name FROM classes WHERE name != 'Thelma Fletcher') tbl GROUP BY majors, submajors, name;
--Testcase 620:
SELECT majors, submajors, name FROM (SELECT courses->>'majors' AS majors, courses->>'sub-majors' AS submajors, name FROM classes WHERE name != 'Thelma Fletcher') tbl GROUP BY majors, submajors, name;

--Testcase 621:
EXPLAIN VERBOSE 
SELECT name || 'xx', courses->>'majors', "isAtDorm", score FROM (SELECT name, courses, "isAtDorm", score FROM classes WHERE id = 1 OR id = 7) tbl GROUP BY courses, name, score, "isAtDorm" ORDER BY courses, name, score, "isAtDorm";
--Testcase 622:
SELECT name || 'xx', courses->>'majors', "isAtDorm", score FROM (SELECT name, courses, "isAtDorm", score FROM classes WHERE id = 1 OR id = 7) tbl GROUP BY courses, name, score, "isAtDorm" ORDER BY courses, name, score, "isAtDorm";

--Testcase 623:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM (SELECT name, id, score FROM classes WHERE id > 5 AND score < 0) tbl GROUP BY (id + score), name, score;
--Testcase 624:
SELECT name, id + score, score FROM (SELECT name, id, score FROM classes WHERE id > 5 AND score < 0) tbl GROUP BY (id + score), name, score;

--GROUP BY, LIMIT (where)
--Testcase 625:
EXPLAIN VERBOSE 
SELECT * FROM classes WHERE id < 5 GROUP BY id, name, courses, "isAtDorm", score LIMIT 5;
--Testcase 626:
SELECT * FROM classes WHERE id < 5 GROUP BY id, name, courses, "isAtDorm", score LIMIT 5;

--Testcase 627:
EXPLAIN VERBOSE 
SELECT id, name, courses FROM classes WHERE name <> 'Lora Hamilton' GROUP BY id, name, courses LIMIT 3;
--Testcase 628:
SELECT id, name, courses FROM classes WHERE name <> 'Lora Hamilton' GROUP BY id, name, courses LIMIT 3;

--Testcase 629:
EXPLAIN VERBOSE 
SELECT name, "isAtDorm", score FROM classes WHERE score >= 0 GROUP BY name, "isAtDorm", score LIMIT 2;
--Testcase 630:
SELECT name, "isAtDorm", score FROM classes WHERE score >= 0 GROUP BY name, "isAtDorm", score LIMIT 2;

--Testcase 631:
EXPLAIN VERBOSE 
SELECT id, name, score FROM classes GROUP BY id, name, score LIMIT 5;
--Testcase 632:
SELECT id, name, score FROM classes GROUP BY id, name, score LIMIT 5;

--Testcase 633:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id <= 10 GROUP BY courses->'majors', courses->'sub-majors' LIMIT 3;
--Testcase 634:
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id <= 10 GROUP BY courses->'majors', courses->'sub-majors' LIMIT 3;

--Testcase 635:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id BETWEEN 10 AND 15 GROUP BY courses->'majors', courses->'sub-majors' LIMIT ALL;
--Testcase 636:
SELECT courses->'majors', courses->'sub-majors' FROM classes WHERE id BETWEEN 10 AND 15 GROUP BY courses->'majors', courses->'sub-majors' LIMIT ALL;

--Testcase 637:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM classes WHERE id > 5 AND id < 15 GROUP BY name, score, courses ORDER BY name, score, courses LIMIT NULL;
--Testcase 638:
SELECT name, score, courses FROM classes WHERE id > 5 AND id < 15 GROUP BY name, score, courses ORDER BY name, score, courses LIMIT NULL;

--Testcase 639:
EXPLAIN VERBOSE 
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes WHERE id NOT IN (1, 6, 9, 8) GROUP BY courses, name LIMIT 5;
--Testcase 640:
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes WHERE id NOT IN (1, 6, 9, 8) GROUP BY courses, name LIMIT 5;

--Testcase 641:
EXPLAIN VERBOSE 
SELECT name || 'sssssOW', courses->>'majors', "isAtDorm", score FROM classes WHERE score > 0 AND score < 6564.15 GROUP BY name, courses, "isAtDorm", score ORDER BY name, courses, "isAtDorm", score LIMIT 3;
--Testcase 642:
SELECT name || 'sssssOW', courses->>'majors', "isAtDorm", score FROM classes WHERE score > 0 AND score < 6564.15 GROUP BY name, courses, "isAtDorm", score ORDER BY name, courses, "isAtDorm", score LIMIT 3;

--Testcase 643:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM classes WHERE score >= 0 GROUP BY name, id, score LIMIT 1;
--Testcase 644:
SELECT name, id + score, score FROM classes WHERE score >= 0 GROUP BY name, id, score LIMIT 1;
--GROUP BY, LIMIT

--Testcase 645:
EXPLAIN VERBOSE 
SELECT id, name, score + id FROM classes GROUP BY id, name, score LIMIT 5;
--Testcase 646:
SELECT id, name, score + id FROM classes GROUP BY id, name, score LIMIT 5;

--Testcase 647:
EXPLAIN VERBOSE 
SELECT id, name, id * score + score/id FROM classes GROUP BY id, name, score LIMIT ALL;
--Testcase 648:
SELECT id, name, id * score + score/id FROM classes GROUP BY id, name, score LIMIT ALL;

--Testcase 649:
EXPLAIN VERBOSE 
SELECT name, "isAtDorm", score FROM classes GROUP BY name, "isAtDorm", score LIMIT 3;
--Testcase 650:
SELECT name, "isAtDorm", score FROM classes GROUP BY name, "isAtDorm", score LIMIT 3;

--Testcase 651:
EXPLAIN VERBOSE 
SELECT id, name, score FROM classes GROUP BY id, name, score LIMIT 3;
--Testcase 652:
SELECT id, name, score FROM classes GROUP BY id, name, score LIMIT 3;

--Testcase 653:
EXPLAIN VERBOSE 
SELECT name, id, score, "isAtDorm" FROM classes GROUP BY name, id, score, "isAtDorm" LIMIT 5;
--Testcase 654:
SELECT name, id, score, "isAtDorm" FROM classes GROUP BY name, id, score, "isAtDorm" LIMIT 5;

--Testcase 655:
EXPLAIN VERBOSE 
SELECT courses->'majors', courses->'sub-majors' FROM classes GROUP BY courses LIMIT NULL;
--Testcase 656:
SELECT courses->'majors', courses->'sub-majors' FROM classes GROUP BY courses LIMIT NULL;

--Testcase 657:
EXPLAIN VERBOSE 
SELECT name, score, courses FROM classes GROUP BY name, score, courses LIMIT 5;
--Testcase 658:
SELECT name, score, courses FROM classes GROUP BY name, score, courses LIMIT 5;

--Testcase 659:
EXPLAIN VERBOSE 
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes GROUP BY courses, name LIMIT 5;
--Testcase 660:
SELECT courses->>'majors', courses->>'sub-majors', name FROM classes GROUP BY courses, name LIMIT 5;

--Testcase 661:
EXPLAIN VERBOSE 
SELECT name || 'wefjwf', courses->>'majors', "isAtDorm", score FROM classes GROUP BY name, courses, "isAtDorm", score LIMIT 5;
--Testcase 662:
SELECT name || 'wefjwf', courses->>'majors', "isAtDorm", score FROM classes GROUP BY name, courses, "isAtDorm", score LIMIT 5;

--Testcase 663:
EXPLAIN VERBOSE 
SELECT name, id + score, score FROM classes GROUP BY (id + score), name, score LIMIT 5;
--Testcase 664:
SELECT name, id + score, score FROM classes GROUP BY (id + score), name, score LIMIT 5;

-- Test INSERT/UPDATE/DELETE with RETURNING and RUNTURNING * 
-- support RETURNING in UPDATE/DELETE operation
--Testcase 671:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = 10 RETURNING id, "isAtDorm", score, courses;
--Testcase 672:
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = 10 RETURNING id, "isAtDorm", score, courses;

--Testcase 673:
EXPLAIN VERBOSE 
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = 10 RETURNING *;
--Testcase 674:
UPDATE classes SET courses = '{"majors": "Environment"}' WHERE id = 10 RETURNING *;


--Testcase 675:
EXPLAIN VERBOSE 
DELETE FROM classes WHERE id = 11 RETURNING id, "isAtDorm", score, courses;
--Testcase 676:
DELETE FROM classes WHERE id = 11 RETURNING id, "isAtDorm", score, courses;

--Testcase 677:
EXPLAIN VERBOSE 
DELETE FROM classes WHERE id = 12 RETURNING *;
--Testcase 678:
DELETE FROM classes WHERE id = 12 RETURNING *;

-- Not support RETURNING in INSERT operation
--Testcase 679:
EXPLAIN VERBOSE 
INSERT INTO classes (id, "isAtDorm", score, courses) VALUES (30, true, 100.001, '{"majors": "CNTT", "sub-majors": "Stream Game"}') RETURNING id, "isAtDorm", score, courses;
--Testcase 680:
INSERT INTO classes (id, "isAtDorm", score, courses) VALUES (30, true, 100.001, '{"majors": "CNTT", "sub-majors": "Stream Game"}') RETURNING id, "isAtDorm", score, courses;

--Testcase 681:
EXPLAIN VERBOSE 
INSERT INTO classes (id, "isAtDorm", score, courses) VALUES (30, true, 100.001, '{"majors": "CNTT", "sub-majors": "Stream Game"}') RETURNING *;
--Testcase 682:
INSERT INTO classes (id, "isAtDorm", score, courses) VALUES (30, true, 100.001, '{"majors": "CNTT", "sub-majors": "Stream Game"}') RETURNING *;


--Testcase 665:
DROP FOREIGN TABLE classes;
--Testcase 666:
DROP FOREIGN TABLE J1_TBL;
--Testcase 667:
DROP FOREIGN TABLE J2_TBL;

--Testcase 668:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 669:
DROP SERVER dynamodb_server;
--Testcase 670:
DROP EXTENSION dynamodb_fdw;

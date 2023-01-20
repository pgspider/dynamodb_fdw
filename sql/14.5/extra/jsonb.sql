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
 SERVER dynamodb_server OPTIONS (table_name 'rows_jsonb', partition_key 'ID');

--Testcase 5:
EXPLAIN VERBOSE
SELECT jsonb_agg(q ORDER BY x, y)
  FROM rows q;
--Testcase 6:
SELECT jsonb_agg(q ORDER BY x, y)
  FROM rows q;

--Testcase 7:
EXPLAIN VERBOSE
UPDATE rows SET x = NULL WHERE x = 1;
--Testcase 8:
UPDATE rows SET x = NULL WHERE x = 1;

--Testcase 9:
EXPLAIN VERBOSE
SELECT jsonb_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;
--Testcase 10:
SELECT jsonb_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;

-- jsonb extraction functions
--Testcase 11:
CREATE FOREIGN TABLE test_jsonb ("ID" int, json_type text, test_json jsonb)
 SERVER dynamodb_server OPTIONS (table_name 'test_jsonb', partition_key 'ID');

--Testcase 12:
EXPLAIN VERBOSE
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 13:
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'scalar';

--Testcase 14:
EXPLAIN VERBOSE
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'array';
--Testcase 15:
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'array';

--Testcase 16:
EXPLAIN VERBOSE
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'object';
--Testcase 17:
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'object';

--Testcase 18:
EXPLAIN VERBOSE
SELECT test_json -> 'field2' FROM test_jsonb WHERE json_type = 'object';
--Testcase 19:
SELECT test_json -> 'field2' FROM test_jsonb WHERE json_type = 'object';

--Testcase 20:
EXPLAIN VERBOSE
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 21:
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'scalar';

--Testcase 22:
EXPLAIN VERBOSE
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'array';
--Testcase 23:
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'array';

--Testcase 24:
EXPLAIN VERBOSE
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'object';
--Testcase 25:
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'object';

--Testcase 26:
EXPLAIN VERBOSE
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 27:
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'scalar';

--Testcase 28:
EXPLAIN VERBOSE
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'array';
--Testcase 29:
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'array';

--Testcase 30:
EXPLAIN VERBOSE
SELECT test_json -> 9 FROM test_jsonb WHERE json_type = 'array';
--Testcase 31:
SELECT test_json -> 9 FROM test_jsonb WHERE json_type = 'array';

--Testcase 32:
EXPLAIN VERBOSE
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'object';
--Testcase 33:
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'object';

--Testcase 34:
EXPLAIN VERBOSE
SELECT test_json ->> 6 FROM test_jsonb WHERE json_type = 'array';
--Testcase 35:
SELECT test_json ->> 6 FROM test_jsonb WHERE json_type = 'array';

--Testcase 36:
EXPLAIN VERBOSE
SELECT test_json ->> 7 FROM test_jsonb WHERE json_type = 'array';
--Testcase 37:
SELECT test_json ->> 7 FROM test_jsonb WHERE json_type = 'array';

--Testcase 38:
EXPLAIN VERBOSE
SELECT test_json ->> 'field4' FROM test_jsonb WHERE json_type = 'object';
--Testcase 39:
SELECT test_json ->> 'field4' FROM test_jsonb WHERE json_type = 'object';

--Testcase 40:
EXPLAIN VERBOSE
SELECT test_json ->> 'field5' FROM test_jsonb WHERE json_type = 'object';
--Testcase 41:
SELECT test_json ->> 'field5' FROM test_jsonb WHERE json_type = 'object';

--Testcase 42:
EXPLAIN VERBOSE
SELECT test_json ->> 'field6' FROM test_jsonb WHERE json_type = 'object';
--Testcase 43:
SELECT test_json ->> 'field6' FROM test_jsonb WHERE json_type = 'object';

--Testcase 44:
EXPLAIN VERBOSE
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 45:
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'scalar';

--Testcase 46:
EXPLAIN VERBOSE
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'array';
--Testcase 47:
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'array';

--Testcase 48:
EXPLAIN VERBOSE
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'object';
--Testcase 49:
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'object';

--Testcase 50:
EXPLAIN VERBOSE
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 51:
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'scalar';

--Testcase 52:
EXPLAIN VERBOSE
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'array';
--Testcase 53:
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'array';

--Testcase 54:
EXPLAIN VERBOSE
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'object';
--Testcase 55:
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'object';


-- nulls
--Testcase 56:
EXPLAIN VERBOSE
SELECT (test_json->'field3') IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'object';
--Testcase 57:
SELECT (test_json->'field3') IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'object';

--Testcase 58:
EXPLAIN VERBOSE
SELECT (test_json->>'field3') IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'object';
--Testcase 59:
SELECT (test_json->>'field3') IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'object';

--Testcase 60:
EXPLAIN VERBOSE
SELECT (test_json->3) IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'array';
--Testcase 61:
SELECT (test_json->3) IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'array';

--Testcase 62:
EXPLAIN VERBOSE
SELECT (test_json->>3) IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'array';
--Testcase 63:
SELECT (test_json->>3) IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'array';


-- array exists - array elements should behave as keys
--Testcase 64:
CREATE FOREIGN TABLE testjsonb ("ID" int, j jsonb)
 SERVER dynamodb_server OPTIONS (table_name 'testjsonb', partition_key 'ID');

--Testcase 65:
EXPLAIN VERBOSE
SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';
--Testcase 66:
SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';

-- type sensitive array exists - should return no rows (since "exists" only
-- matches strings that are either object keys or array elements)
--Testcase 67:
EXPLAIN VERBOSE
SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;
--Testcase 68:
SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;

-- However, a raw scalar is *contained* within the array
--Testcase 69:
EXPLAIN VERBOSE
SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;
--Testcase 70:
SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;

-- indexing
--Testcase 71:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 72:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';

--Testcase 73:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 74:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';

--Testcase 75:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 76:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';

--Testcase 77:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 78:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';

--Testcase 79:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 80:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';

--Testcase 81:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ? 'public';
--Testcase 82:
SELECT count(*) FROM testjsonb WHERE j ? 'public';

--Testcase 83:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ? 'bar';
--Testcase 84:
SELECT count(*) FROM testjsonb WHERE j ? 'bar';

--Testcase 85:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];
--Testcase 86:
SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];

--Testcase 87:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];
--Testcase 88:
SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];

--Testcase 89:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 90:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';

--Testcase 91:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 92:
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';

--Testcase 93:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 94:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';

--Testcase 95:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 96:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';

--Testcase 97:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 98:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';

--Testcase 99:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 100:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';

--Testcase 101:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';
--Testcase 102:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';

--Testcase 103:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';
--Testcase 104:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';

--Testcase 105:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';
--Testcase 106:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';

--Testcase 107:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';
--Testcase 108:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';

--Testcase 109:
EXPLAIN VERBOSE
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 110:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';

--Testcase 111:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 112:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';

--Testcase 113:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 114:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';

--Testcase 115:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 116:
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';

--Testcase 117:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 118:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';

--Testcase 119:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 120:
SELECT count(*) FROM testjsonb WHERE j @? '$';

--Testcase 121:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 122:
SELECT count(*) FROM testjsonb WHERE j @? '$.public';

--Testcase 123:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';
--Testcase 124:
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';


--Testcase 125:
CREATE INDEX jidx ON testjsonb USING gin (j);
SET enable_seqscan = off;

--Testcase 126:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 127:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';

--Testcase 128:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 129:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';

--Testcase 130:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 131:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';

--Testcase 132:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 133:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';

--Testcase 134:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 135:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';

--Testcase 136:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"array":["foo"]}';
--Testcase 137:
SELECT count(*) FROM testjsonb WHERE j @> '{"array":["foo"]}';

--Testcase 138:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"array":["bar"]}';
--Testcase 139:
SELECT count(*) FROM testjsonb WHERE j @> '{"array":["bar"]}';

-- exercise GIN_SEARCH_MODE_ALL
--Testcase 140:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{}';
--Testcase 141:
SELECT count(*) FROM testjsonb WHERE j @> '{}';

--Testcase 142:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ? 'public';
--Testcase 143:
SELECT count(*) FROM testjsonb WHERE j ? 'public';

--Testcase 144:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ? 'bar';
--Testcase 145:
SELECT count(*) FROM testjsonb WHERE j ? 'bar';

--Testcase 146:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];
--Testcase 147:
SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];

--Testcase 148:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];
--Testcase 149:
SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];

--Testcase 150:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 151:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';

--Testcase 152:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 153:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';

--Testcase 154:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';
--Testcase 155:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';

--Testcase 156:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';
--Testcase 157:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';

--Testcase 158:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 159:
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';

--Testcase 160:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 161:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';

--Testcase 162:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 163:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';

--Testcase 164:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 165:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';

--Testcase 166:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';
--Testcase 167:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';

--Testcase 168:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';
--Testcase 169:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';

--Testcase 170:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';
--Testcase 171:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';

--Testcase 172:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';
--Testcase 173:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';

--Testcase 174:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';
--Testcase 175:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';

--Testcase 176:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 177:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';

--Testcase 178:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';
--Testcase 179:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';

--Testcase 180:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';
--Testcase 181:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';

--Testcase 182:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';
--Testcase 183:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';

--Testcase 184:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';
--Testcase 185:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';

--Testcase 186:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 187:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';

--Testcase 188:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 189:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';

--Testcase 190:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 191:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';

--Testcase 192:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 193:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';

--Testcase 194:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 195:
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';

--Testcase 196:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 197:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';

--Testcase 198:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';
--Testcase 199:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';

--Testcase 200:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';
--Testcase 201:
SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';

--Testcase 202:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';
--Testcase 203:
SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';

--Testcase 204:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 205:
SELECT count(*) FROM testjsonb WHERE j @? '$';

--Testcase 206:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 207:
SELECT count(*) FROM testjsonb WHERE j @? '$.public';

--Testcase 208:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';
--Testcase 209:
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';


-- array exists - array elements should behave as keys (for GIN index scans too)
--Testcase 210:
CREATE INDEX jidx_array ON testjsonb USING gin((j->'array'));

--Testcase 211:
EXPLAIN VERBOSE
SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';
--Testcase 212:
SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';

-- type sensitive array exists - should return no rows (since "exists" only
-- matches strings that are either object keys or array elements)
--Testcase 213:
EXPLAIN VERBOSE
SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;
--Testcase 214:
SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;

-- However, a raw scalar is *contained* within the array
--Testcase 215:
EXPLAIN VERBOSE
SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;
--Testcase 216:
SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;


RESET enable_seqscan;

--Testcase 217:
EXPLAIN VERBOSE
SELECT count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow;
--Testcase 218:
SELECT count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow;

--Testcase 219:
EXPLAIN VERBOSE
SELECT key, count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow GROUP BY key ORDER BY count DESC, key;
--Testcase 220:
SELECT key, count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow GROUP BY key ORDER BY count DESC, key;


-- sort/hash
--Testcase 221:
EXPLAIN VERBOSE
SELECT count(distinct j) FROM testjsonb;
--Testcase 222:
SELECT count(distinct j) FROM testjsonb;

SET enable_hashagg = off;

--Testcase 223:
EXPLAIN VERBOSE
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;
--Testcase 224:
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;

SET enable_hashagg = on;
SET enable_sort = off;

--Testcase 225:
EXPLAIN VERBOSE
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;
--Testcase 226:
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;

--Testcase 227:
EXPLAIN VERBOSE
SELECT distinct * FROM (values (jsonb '{}' || ''::text),('{}')) v(j);
--Testcase 228:
SELECT distinct * FROM (values (jsonb '{}' || ''::text),('{}')) v(j);

SET enable_sort = on;

RESET enable_hashagg;
RESET enable_sort;

-- btree
--Testcase 229:
CREATE INDEX jidx ON testjsonb USING btree (j);
SET enable_seqscan = off;

--Testcase 230:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j > '{"p":1}';
--Testcase 231:
SELECT count(*) FROM testjsonb WHERE j > '{"p":1}';

--Testcase 232:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j = '{"pos":98, "line":371, "node":"CBA", "indexed":true}';
--Testcase 233:
SELECT count(*) FROM testjsonb WHERE j = '{"pos":98, "line":371, "node":"CBA", "indexed":true}';


--gin path opclass
--Testcase 234:
DROP INDEX jidx;
--Testcase 235:
CREATE INDEX jidx ON testjsonb USING gin (j jsonb_path_ops);
SET enable_seqscan = off;

--Testcase 236:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 237:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';

--Testcase 238:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 239:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';

--Testcase 240:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 241:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';

--Testcase 242:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 243:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';

--Testcase 244:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 245:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';

-- exercise GIN_SEARCH_MODE_ALL
--Testcase 246:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @> '{}';
--Testcase 247:
SELECT count(*) FROM testjsonb WHERE j @> '{}';

--Testcase 248:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 249:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';

--Testcase 250:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';
--Testcase 251:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';

--Testcase 252:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';
--Testcase 253:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';

--Testcase 254:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 255:
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';

--Testcase 256:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 257:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';

--Testcase 258:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 259:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';

--Testcase 260:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 261:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';

--Testcase 262:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';
--Testcase 263:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';

--Testcase 264:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';
--Testcase 265:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';

--Testcase 266:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';
--Testcase 267:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';

--Testcase 268:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';
--Testcase 269:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';

--Testcase 270:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';
--Testcase 271:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';

--Testcase 272:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 273:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';

--Testcase 274:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 275:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';

--Testcase 276:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 277:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';

--Testcase 278:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 279:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';

--Testcase 280:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 281:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';

--Testcase 282:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 283:
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';

--Testcase 284:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 285:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';

--Testcase 286:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';
--Testcase 287:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';

--Testcase 288:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';
--Testcase 289:
SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';

--Testcase 290:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';
--Testcase 291:
SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';

--Testcase 292:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 293:
SELECT count(*) FROM testjsonb WHERE j @? '$';

--Testcase 294:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 295:
SELECT count(*) FROM testjsonb WHERE j @? '$.public';

--Testcase 296:
EXPLAIN VERBOSE
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';
--Testcase 297:
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';

RESET enable_seqscan;
--Testcase 298:
DROP INDEX jidx;


--Testcase 299:
CREATE FOREIGN TABLE foo (serial_num int, name text, type text)
 SERVER dynamodb_server OPTIONS (table_name 'foo_jsonb', partition_key 'serial_num');

--Testcase 300:
EXPLAIN VERBOSE
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;
--Testcase 301:
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;

--Testcase 302:
EXPLAIN VERBOSE
SELECT json_object_agg(name, type) FROM foo;
--Testcase 303:
SELECT json_object_agg(name, type) FROM foo;

--Testcase 304:
EXPLAIN VERBOSE
INSERT INTO foo VALUES (999999, NULL, 'bar');
--Testcase 305:
INSERT INTO foo VALUES (999999, NULL, 'bar');

--Testcase 306:
EXPLAIN VERBOSE
SELECT json_object_agg(name, type) FROM foo;
--Testcase 307:
SELECT json_object_agg(name, type) FROM foo;



-- populate_record
--Testcase 308:
create type jpop as (a text, b int, c timestamp);

--Testcase 309:
CREATE DOMAIN js_int_array_1d  AS int[]   CHECK(array_length(VALUE, 1) = 3);
--Testcase 310:
CREATE DOMAIN js_int_array_2d  AS int[][] CHECK(array_length(VALUE, 2) = 3);

--Testcase 311:
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
--Testcase 312:
CREATE FOREIGN TABLE jspoptest ("ID" int, js json)
 SERVER dynamodb_server OPTIONS (table_name 'jspoptest', partition_key 'ID');

--Testcase 313:
EXPLAIN VERBOSE
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;
--Testcase 314:
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;


--Testcase 315:
DROP TYPE jsrec;
--Testcase 316:
DROP TYPE jsrec_i_not_null;
--Testcase 317:
DROP DOMAIN js_int_array_1d;
--Testcase 318:
DROP DOMAIN js_int_array_2d;

--Testcase 319:
CREATE FOREIGN TABLE nestjsonb ("ID" int, j jsonb)
 SERVER dynamodb_server OPTIONS (table_name 'nestjsonb', partition_key 'ID');

--Testcase 320:
create index on nestjsonb using gin(j jsonb_path_ops);

set enable_seqscan = on;
set enable_bitmapscan = off;

--Testcase 321:
EXPLAIN VERBOSE
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;
--Testcase 322:
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;

--Testcase 323:
EXPLAIN VERBOSE
select * from nestjsonb where j @> '{"c":3}';
--Testcase 324:
select * from nestjsonb where j @> '{"c":3}';

--Testcase 325:
EXPLAIN VERBOSE
select * from nestjsonb where j @> '[[14]]';
--Testcase 326:
select * from nestjsonb where j @> '[[14]]';

set enable_seqscan = off;
set enable_bitmapscan = on;

--Testcase 327:
EXPLAIN VERBOSE
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;
--Testcase 328:
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;

--Testcase 329:
EXPLAIN VERBOSE
select * from nestjsonb where j @> '{"c":3}';
--Testcase 330:
select * from nestjsonb where j @> '{"c":3}';

--Testcase 331:
EXPLAIN VERBOSE
select * from nestjsonb where j @> '[[14]]';
--Testcase 332:
select * from nestjsonb where j @> '[[14]]';

reset enable_seqscan;
reset enable_bitmapscan;

--Testcase 335:
CREATE FOREIGN TABLE test_jsonb_subscript ("id" int, test_json jsonb)
 SERVER dynamodb_server OPTIONS (table_name 'test_jsonb_subscript', partition_key 'id');

--Testcase 336:
insert into test_jsonb_subscript values
(1, '{}'), -- empty jsonb
(2, '{"key": "value"}'); -- jsonb with data

-- use jsonb subscription in where clause
--Testcase 337:
select * from test_jsonb_subscript where test_json['key'] = '"value"';
--Testcase 338:
select * from test_jsonb_subscript where test_json['key_doesnt_exists'] = '"value"';
--Testcase 339:
select * from test_jsonb_subscript where test_json['key'] = '"wrong_value"';

--Testcase 333:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 334:
DROP EXTENSION dynamodb_fdw CASCADE;

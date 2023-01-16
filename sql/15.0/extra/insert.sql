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
-- insert with DEFAULT in the target_list
--
--Testcase 4:
CREATE FOREIGN TABLE inserttest ("ID" int, col1 int4, col2 int4, col3 text)
  SERVER dynamodb_server OPTIONS (table_name 'inserttest', partition_key 'ID');

--Testcase 5:
EXPLAIN VERBOSE
insert into inserttest ("ID", col1, col2, col3) values (1, DEFAULT, DEFAULT, DEFAULT);
--Testcase 6:
insert into inserttest ("ID", col1, col2, col3) values (1, DEFAULT, DEFAULT, DEFAULT);

--Testcase 7:
EXPLAIN VERBOSE
insert into inserttest ("ID", col2, col3) values (2, 3, DEFAULT);
--Testcase 8:
insert into inserttest ("ID", col2, col3) values (2, 3, DEFAULT);

--Testcase 9:
EXPLAIN VERBOSE
insert into inserttest ("ID", col1, col2, col3) values (3, DEFAULT, 5, DEFAULT);
--Testcase 10:
insert into inserttest ("ID", col1, col2, col3) values (3, DEFAULT, 5, DEFAULT);

--Testcase 11:
EXPLAIN VERBOSE
insert into inserttest values (4, DEFAULT, 5, 'test');
--Testcase 12:
insert into inserttest values (4, DEFAULT, 5, 'test');

--Testcase 13:
EXPLAIN VERBOSE
insert into inserttest values (5, DEFAULT, 7);
--Testcase 14:
insert into inserttest values (5, DEFAULT, 7);

--Testcase 15:
EXPLAIN VERBOSE
select * from inserttest;
--Testcase 16:
select * from inserttest;


--
-- insert with similar expression / target_list values (all fail)
--
--Testcase 17:
EXPLAIN VERBOSE
insert into inserttest ("ID", col1, col2, col3) values (6, DEFAULT, DEFAULT);
--Testcase 18:
insert into inserttest ("ID", col1, col2, col3) values (6, DEFAULT, DEFAULT);

--Testcase 19:
EXPLAIN VERBOSE
insert into inserttest ("ID", col1, col2, col3) values (7, 1, 2);
--Testcase 20:
insert into inserttest ("ID", col1, col2, col3) values (7, 1, 2);

--Testcase 21:
EXPLAIN VERBOSE
insert into inserttest ("ID", col1) values (8, 1, 2);
--Testcase 22:
insert into inserttest ("ID", col1) values (8, 1, 2);

--Testcase 23:
EXPLAIN VERBOSE
insert into inserttest ("ID", col1) values (9, DEFAULT, DEFAULT);
--Testcase 24:
insert into inserttest ("ID", col1) values (9, DEFAULT, DEFAULT);

--Testcase 25:
select * from inserttest;

--
-- VALUES test
--
--Testcase 26:
EXPLAIN VERBOSE
insert into inserttest values(10, 10, 20, '40'), (11, -1, 2, DEFAULT),
    (11, (select 2), (select i from (values(3)) as foo (i)), 'values are fun!');
    
--Testcase 27:
EXPLAIN VERBOSE
insert into inserttest values(10, 10, 20, '40'), (11, -1, 2, DEFAULT),
    (11, (select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

--Testcase 28:
select * from inserttest;

--
-- TOASTed value test
--
--Testcase 29:
EXPLAIN VERBOSE
insert into inserttest values(12, 30, 50, repeat('x', 10000));
--Testcase 30:
insert into inserttest values(12, 30, 50, repeat('x', 10000));

--Testcase 31:
EXPLAIN VERBOSE
select col1, col2, char_length(col3) from inserttest;
--Testcase 32:
select col1, col2, char_length(col3) from inserttest;


--Testcase 33:
drop foreign table inserttest;

--Testcase 34:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 35:
DROP EXTENSION dynamodb_fdw CASCADE;

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


-- Create foreign tables and validate
--Testcase 4:
CREATE FOREIGN TABLE connection_tbl (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'connection_tbl', partition_key 'artist', sort_key 'songtitle');

--Testcase 5:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;


-- Drop foreign table and create it again without partition_key and sort_key
--Testcase 6:
DROP FOREIGN TABLE connection_tbl;
--Testcase 7:
CREATE FOREIGN TABLE connection_tbl (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'connection_tbl');
--Testcase 8:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;
-- Should fail with an error
--Testcase 9:
INSERT INTO connection_tbl VALUES ('9', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 10:
UPDATE connection_tbl SET albumtitle = 'RECORD UPDATED' WHERE artist = '9';
--Testcase 11:
DELETE FROM connection_tbl WHERE artist = '9';
--Testcase 12:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;


-- Drop foreign table and create it again with invalid partition_key and valid sort_key
--Testcase 13:
DROP FOREIGN TABLE connection_tbl;
--Testcase 14:
CREATE FOREIGN TABLE connection_tbl (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'connection_tbl', partition_key 'wrong', sort_key 'songtitle');
--Testcase 15:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;
-- Should fail with an error
--Testcase 16:
INSERT INTO connection_tbl VALUES ('8', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 17:
UPDATE connection_tbl SET albumtitle = 'RECORD UPDATED' WHERE artist = '8';
--Testcase 18:
DELETE FROM connection_tbl WHERE artist = '8';
--Testcase 19:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;

-- Drop foreign table and create it again with valid partition_key and invalid sort_key
--Testcase 20:
DROP FOREIGN TABLE connection_tbl;
--Testcase 21:
CREATE FOREIGN TABLE connection_tbl (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'connection_tbl', partition_key 'artist', sort_key 'wrong');
-- Should fail with an error
--Testcase 22:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;
--Testcase 23:
INSERT INTO connection_tbl VALUES ('8', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
-- Should fail to update/delete the data with an error
--Testcase 24:
UPDATE connection_tbl SET albumtitle = 'RECORD UPDATED' WHERE artist = '8';
--Testcase 25:
DELETE FROM connection_tbl WHERE artist = '8';
--Testcase 26:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;


-- Alter one of the SERVER option
-- Set correct partition_key and sort_key for dynamodb_server
--Testcase 27:
DROP FOREIGN TABLE connection_tbl;
--Testcase 28:
CREATE FOREIGN TABLE connection_tbl (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'connection_tbl', partition_key 'artist', sort_key 'songtitle');
-- Set wrong endpoint for dynamodb_server
ALTER SERVER dynamodb_server OPTIONS (SET endpoint 'http://localhost:6868');
-- Should fail with an error
--Testcase 29:
INSERT INTO connection_tbl VALUES ('1', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 30:
UPDATE connection_tbl SET albumtitle = 'RECORD UPDATED' WHERE artist = '1';
--Testcase 31:
DELETE FROM connection_tbl WHERE artist = '1';
--Testcase 32:
SELECT artist, songtitle, albumtitle FROM connection_tbl ORDER BY 1, 2;
-- Set correct address for dynamodb_server
ALTER SERVER dynamodb_server OPTIONS (SET endpoint :DYNAMODB_ENDPOINT);
-- Should able to insert the data
--Testcase 33:
INSERT INTO connection_tbl VALUES ('1', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 34:
DELETE FROM connection_tbl WHERE artist = '1';


-- Drop user mapping and create with invalid user and password for public
-- user mapping
--Testcase 35:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 36:
CREATE USER MAPPING FOR public SERVER dynamodb_server
  OPTIONS (user 'wrong', password 'wrong');
-- Should fail with an error
--Testcase 37:
INSERT INTO connection_tbl VALUES ('2', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 38:
UPDATE connection_tbl SET albumtitle = 'RECORD UPDATED' WHERE artist = '2';
--Testcase 39:
DELETE FROM connection_tbl WHERE artist = '2';
-- Drop user mapping and create without username and password for public
-- user mapping
--Testcase 40:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 41:
CREATE USER MAPPING FOR public SERVER dynamodb_server;
-- Should fail with an error
--Testcase 42:
INSERT INTO connection_tbl VALUES ('3', 'SomE oNe LiKE yOu', 'RECORD INSERTED');
--Testcase 43:
DELETE FROM connection_tbl WHERE artist = '3';

--Testcase 44:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 45:
DROP EXTENSION dynamodb_fdw CASCADE;

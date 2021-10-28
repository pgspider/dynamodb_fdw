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


-- Validate extension, server and mapping details
--Testcase 4:
SELECT e.fdwname AS "Extension", srvname AS "Server", s.srvoptions AS "Server_Options", u.umoptions AS "User_Mapping_Options"
  FROM pg_foreign_data_wrapper e LEFT JOIN pg_foreign_server s ON e.oid = s.srvfdw LEFT JOIN pg_user_mapping u ON s.oid = u.umserver
  WHERE e.fdwname = 'dynamodb_fdw'
  ORDER BY 1, 2, 3, 4;

-- Create foreign tables and perform basic SQL operations
--Testcase 5:
CREATE FOREIGN TABLE server_option_tbl (artist text, songtitle text, albumtitle text)
  SERVER dynamodb_server OPTIONS (table_name 'server_option_tbl', partition_key 'artist', sort_key 'songtitle');
--Testcase 6:
SELECT artist, songtitle, albumtitle FROM server_option_tbl ORDER BY 1, 2;
--Testcase 7:
INSERT INTO server_option_tbl VALUES ('0000', 'nOBodY LiKE mE', 'RECORD INSERTED');
--Testcase 8:
SELECT artist, songtitle, albumtitle FROM server_option_tbl ORDER BY 1, 2;
--Testcase 9:
UPDATE server_option_tbl SET albumtitle = 'RECORD UPDATED' WHERE artist = '0000';
--Testcase 10:
SELECT artist, songtitle, albumtitle FROM server_option_tbl ORDER BY 1, 2;
--Testcase 11:
DELETE FROM server_option_tbl WHERE artist = '0000';
--Testcase 12:
SELECT artist, songtitle, albumtitle FROM server_option_tbl ORDER BY 1, 2;

-- Cleanup
--Testcase 13:
DROP FOREIGN TABLE server_option_tbl;
--Testcase 14:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 15:
DROP SERVER dynamodb_server;
--Testcase 16:
DROP EXTENSION dynamodb_fdw;

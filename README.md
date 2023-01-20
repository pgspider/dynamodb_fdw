# DynamoDB Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for DynamoDB.

## 1. Requirement
1. AWS C++ SDK<br>
DynamoDB FDW uses the APIs provided by AWS C++ SDK to connect and execute query on DynamoDB.<br>
It requires gcc version 4.9.0 and above to be able to use and compile.<br>
It also requires 3rd party libraries: libcurl, openssl, libuuid, pulseaudio-libs. <br>
2. Java Runtime Environment (JRE) version 8.x or newer<br>
If using DynamoDB local, JRE 8.x or newer is required.<br>
## 2. Installation guide
This section describle how to install required library on CentOS 7
1. AWS C++ SDK<br>
    Download and follow the Amazon developer guide.
    <pre>https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup-linux.html</pre>

2. Java Runtime Environment (JRE) version 8.x or newer
	<pre>
    $ sudo yum install java-1.8.0-openjdk-devel.x86_64</pre>
3. Build and install PostgreSQL: from PostgreSQL directory
    <pre>
    ./configure
    $ make
    $ make install</pre>
4. Build and install DynamoDB FDW:
    clone source code under PostgreSQL/contrib/
    <pre>
    $ make
    $ make install</pre>
## 3. FDW options
DynamoDB FDW supports the following options:<br>

| No | Option name |Context | Required | Description |
|----|-------------|--------|----------|-------------|
|1   |endpoint|SERVER|Optional|The URL of the entry point for an AWS web service. If user does not specify this option, the endpoint is set to "http://localhost:8000" by default. It is required for AWS DynamoDB and optional for DynamoDB local.
|2   |user|USER MAPPING|Optional|The user credential to connect to DynamoDB. It is required for AWS DynamoDB and optional for DynamoDB local.
|3   |password|USER MAPPING|Optional|The password credential to connect to DynamoDB. It is required for AWS DynamoDB and optional for DynamoDB local.
|4   |partition_key|FOREIGN TABLE|Optional|The column name of the partition key of DynamoDB table.
|5   |sort_key|FOREIGN TABLE|Optional|The column name of the sort key of DynamoDB table.
|6   |table_name|FOREIGN TABLE|Optional|The corresponding table name in DynamoDB.
|7   |column_name|ATTRIBUTE|Optional|The corresponding column name in DynamoDB.
## 5. Usage
* Load extension first time after install:
    <pre>CREATE EXTENSION dynamodb_fdw;</pre>

* Create server object:
    <pre>CREATE SERVER dynamodb_svr FOREIGN DATA WRAPPER dynamodb_fdw OPTIONS (endpoint 'http://localhost:8000');</pre>

* Create user mapping:
    <pre>CREATE USER MAPPING FOR CURRENT_USER SERVER dynamodb_svr OPTIONS (user 'user1', password 'pass');</pre>

* Create foreign table:
    <pre>CREATE FOREIGN TABLE frtbl (c1 int, c2 text, c3 jsonb) SERVER dynamodb_svr OPTIONS (table_name 'table1');</pre>

* Start executing query:
    <pre>SELECT * FROM frtbl;</pre>

## 6. Features
* Support SELECT feature to get data from DynamoDB. DynamoDB FDW supports selecting columns or nested attribute object (using -> or ->> operator)<br>
* Support INSERT feature.
* Support UPDATE feature using foreign modify.
* Support DELETE feature using foreign modify.
* Support push down WHERE clause (including nested attribute object).
* Support push down function SIZE of DynamoDB.

## 7. Limitations
* Does not support List type of DynamoDB.
* Only support SELECT the Binary type of DynamoDB. Does not support WHERE clause, INSERT, UPDATE statement with Binary type of DynamoDB.
* For DynamoDB, 2 records can have the same attribute name but different data type. However, DynamoDB FDW does not support that case. User need to avoid using that case.
* Does not push down WHERE condition when it compares array constant.<br>
For example: `SELECT * FROM array_test WHERE array_n < '{1232, 5121, 8438, 644, 83}';` is not pushed down.
* Does not push down WHERE condition when it contains text comparison using "<, <=, >=, >" operators.<br>
For example: `SELECT * FROM WHERE name > '@#!S';` is not pushed down.
* Does not push down when selecting multiple attributes with the same name.<br>
For example: `SELECT name, friends->'class_info'->'name' FROM students;`
* Does not push down overlap document path. <br>
For example: `SELECT friends->'class_info', friends->'class_info'->'name' FROM students;`
* Does not support COPY FROM and foreign partition routing. The following error will be shown.<br>
    <pre>COPY and foreign partition routing not supported in dynamodb_fdw</pre>
## 8. Notes
* For string set and number set of DynamoDB, the values in the set are sorted from smallest to largest automatically.<br>
Therefore, if you want to access to the element of array, it will return the different value compared to insert value.<br>
For example, you insert `<<3,2,1>>` into DynamoDB. DynamoDB will sort and store it as `<<1,2,3>>`.<br>
If you want to select element `[1]` of the set, it will return 1.
* DynamoDB does not support NULL as element of string set or number set.<br>
Therefore, when user input NULL as element of array, the default value (0 for number, empty string for text) will be inserted into DynamoDB.<br>
For example, user input `array[1,2,null,4]`, the values inserted into DynamoDB will be `[1, 2, 0, 4]`.<br>
User input `array['one','two',null,'four']`, the values inserted into DynamoDB will be `['one', 'two', '', 'four']`.
* If an attribute of Map type does not exist, the condition `xxx IS NULL` will always return false.

## 9. Supported push down operators
### Comparison operators
| No | PostgreSQL | Remark |
|----|------------|--------|
| 1 | `=` | Equal to |
| 2 | `<>` or `!=` | Not Equal to |
| 3 | `> ` | Greater than |
| 4 | `< ` | Less than |
| 5 | `>=` | Greater than or equal to |
| 6 | `<=` | Less than or equal to |

### Logical operators
| No | PostgreSQL | Remark |
|----|------------|--------|
| 1 | `AND` | `TRUE` if all the conditions separated by `AND` are `TRUE` |
| 2 | `BETWEEN` | `TRUE` if the operand is within the range of comparisons |
| 3 | `IN` | Only support `IN` with a list of value. Return `TRUE` if the operand is equal to one of a list of expressions. |
| 4 | `IS` | Only support `IS NULL`. Return `TRUE` if the operand is NULL. |
| 5 | `NOT` | Reverses the value of a given Boolean expression |
| 6 | `OR` | `TRUE` if any of the conditions separated by `OR` are `TRUE` |

### Dereference operators
| No | PostgreSQL | Remark |
|----|------------|--------|
| 1 | `->` | Extracts JSON object field with the given key. This mapping will be used when right operand is an attribute name (which is represented as a text constant). |
| 2 | `->>` | Extracts JSON object field with the given key, as text. |
| 3 | `->` | Extract nested element of List type. This mapping will be used when right operand is a number. |

## 10. Data type mapping between PostgreSQL and DynamoDB

| No | PostgreSQL | DynamoDB | Remark |
|----|------------|----------|--------|
| 1 | boolean | Boolean | N/A | 
| 2 | bytea | Binary | PartiQL of DynamoDB does not have any way to represent binary data. Therefore, DynamoDB FDW only supports selecting Binary column. DynamoDB FDW does not support Binary column in WHERE clause. |
| 3 | JSON/JSONB | Map | N/A |
| 4 | NULL | Null | N/A |
| 5 | smallint, integer, bigint, numeric, real, double precision | Number | N/A |
| 6 | smallint\[\], integer\[\], bigint\[\], numeric\[\], real\[\], double precision\[\] | Number Set | N/A |
| 7 | text character varying(n)\[\], varchar(n)\[\], character(n)\[\], char(n) \[\], text\[\] | String Set | N/A |
| 8 | text character varying(n), varchar(n), character(n), char(n), text | String | N/A |
## 11. License
Copyright and license information can be found in the file [`LICENSE`][1].

[1]: LICENSE

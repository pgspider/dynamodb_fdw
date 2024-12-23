DynamoDB Foreign Data Wrapper for PostgreSQL
============================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [DynamoDB](https://aws.amazon.com/dynamodb/).

<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" align="center" height="100" alt="PostgreSQL"/>	+	**DynamoDB**

Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Data type mapping](#data-type-mapping)
7. [Identifier case handling](#identifier-case-handling)
8. [Generated columns](#generated-columns)
9. [Character set handling](#character-set-handling)
10. [Examples](#examples)
11. [Limitations](#limitations)
12. [Contributing](#contributing)
13. [Useful links](#useful-links)
14. [License](#license)

Features
--------
### Common features

* Support `SELECT` feature to get data from DynamoDB.
* DynamoDB FDW supports selecting columns or nested attribute object (using `->` or `->>` operator)
* Support `INSERT` feature.
* Support `UPDATE` feature using foreign modify.
* Support `DELETE` feature using foreign modify.

### Pushdowning

#### Common pushdowning
* Support push down `WHERE` clause (including nested attribute object).
* Support push down function `SIZE` of DynamoDB.
* Does not push down `WHERE` condition when it compares array constant.
For example: `SELECT * FROM array_test WHERE array_n < '{1232, 5121, 8438, 644, 83}';` is not pushed down.
* Does not push down `WHERE` condition when it contains text comparison using `<, <=, >=, >` operators.
For example: `SELECT * FROM WHERE name > '@#!S';` is not pushed down.
* Does not push down when selecting multiple attributes with the same name.
For example: `SELECT name, friends->'class_info'->'name' FROM students;`
* Does not push down overlap document path. 
For example: `SELECT friends->'class_info', friends->'class_info'->'name' FROM students;`

#### Comparison operators
| No | PostgreSQL | Remark |
|----|------------|--------|
| 1 | `=` | Equal to |
| 2 | `<>` or `!=` | Not Equal to |
| 3 | `> ` | Greater than |
| 4 | `< ` | Less than |
| 5 | `>=` | Greater than or equal to |
| 6 | `<=` | Less than or equal to |

#### Logical operators
| No | PostgreSQL | Remark |
|----|------------|--------|
| 1 | `AND` | `TRUE` if all the conditions separated by `AND` are `TRUE` |
| 2 | `BETWEEN` | `TRUE` if the operand is within the range of comparisons |
| 3 | `IN` | Only support `IN` with a list of value. Return `TRUE` if the operand is equal to one of a list of expressions. |
| 4 | `IS` | Only support `IS NULL`. Return `TRUE` if the operand is NULL. |
| 5 | `NOT` | Reverses the value of a given Boolean expression |
| 6 | `OR` | `TRUE` if any of the conditions separated by `OR` are `TRUE` |

#### Dereference operators
| No | PostgreSQL | Remark |
|----|------------|--------|
| 1 | `->` | Extracts JSON object field with the given key. This mapping will be used when right operand is an attribute name (which is represented as a text constant). |
| 2 | `->>` | Extracts JSON object field with the given key, as text. |
| 3 | `->` | Extract nested element of List type. This mapping will be used when right operand is a number. |

### Notes about features

* For string set and number set of DynamoDB, the values in the set are sorted from smallest to largest automatically.
Therefore, if you want to access to the element of array, it will return the different value compared to insert value.
For example, you insert `<<3,2,1>>` into DynamoDB. DynamoDB will sort and store it as `<<1,2,3>>`.
If you want to select element `[1]` of the set, it will return 1.
* DynamoDB does not support NULL as element of string set or number set.
Therefore, when user input NULL as element of array, the default value (0 for number, empty string for text) will be inserted into DynamoDB.
For example, user input `array[1,2,null,4]`, the values inserted into DynamoDB will be `[1, 2, 0, 4]`.
User input `array['one','two',null,'four']`, the values inserted into DynamoDB will be `['one', 'two', '', 'four']`.
* If an attribute of Map type does not exist, the condition `xxx IS NULL` will always return false.

Also see [Limitations](#limitations).

Supported platforms
-------------------

`dynamodb_fdw` was developed on Linux, and should run on any
reasonably POSIX-compliant system.

`dynamodb_fdw` is designed to be compatible with PostgreSQL 13 ~ 17.0.

Installation
------------
### Prerequisites

1. AWS C++ SDK
DynamoDB FDW uses the APIs provided by AWS C++ SDK to connect and execute query on DynamoDB.
It requires gcc version 4.9.0 and above to be able to use and compile.
It also requires 3rd party libraries: libcurl, openssl, libuuid, pulseaudio-libs. 
2. Java Runtime Environment (JRE) version 8.x or newer
If using DynamoDB local, JRE 8.x or newer is required.

### Source installation

This section describle how to install required library on **CentOS 7**.

1. AWS C++ SDK
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

Usage
-----

## CREATE SERVER options

`dynamodb_fdw` accepts the following options via the `CREATE SERVER` command:

- **endpoint** as *string*, optional, default `http://localhost:8000`

  The URL of the entry point for an AWS web service. It is required for AWS DynamoDB and optional for DynamoDB local.

## CREATE USER MAPPING options

`dynamodb_fdw` accepts the following options via the `CREATE USER MAPPING`
command:

- **user** as *string*, optional, no default

  The user credential to connect to DynamoDB. It is required for AWS DynamoDB and optional for DynamoDB local.

- **password** as *string*, optional, no default

  The password credential to connect to DynamoDB. It is required for AWS DynamoDB and optional for DynamoDB local.


## CREATE FOREIGN TABLE options

`dynamodb_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command.

- **partition_key** as *string*, optional, no default

  The column name of the partition key of DynamoDB table.
  
- **sort_key** as *string*, optional, no default
  
  The column name of the sort key of DynamoDB table.

- **table_name** as *string*, optional, default table name of foreign table

  The corresponding table name in DynamoDB.

The following column-level options are available:

- **column_name** as *string*, optional, default column name of foreign table

  The corresponding column name in DynamoDB.

## IMPORT FOREIGN SCHEMA options

`dynamodb_fdw` **don't support** [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html) and 
 accepts no custom options.

## TRUNCATE support

`dynamodb_fdw` **don't support** the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14.

Functions
---------
As well as the standard `dynamodb_fdw_handler()` and `dynamodb_fdw_validator()`
functions, `dynamodb_fdw` provides the following user-callable utility functions:
Functions from this FDW in PostgreSQL catalog are **yet not described**.

Data type mapping
-----------------

| No | PostgreSQL | DynamoDB | Remark |
|----|------------|----------|--------|
| 1 | boolean | Boolean | N/A | 
| 2 | bytea | Binary | PartiQL of DynamoDB does not have any way to represent binary data. Therefore, DynamoDB FDW only supports selecting Binary column. DynamoDB FDW does not support Binary column in `WHERE` clause. |
| 3 | JSON/JSONB | Map | N/A |
| 4 | NULL | Null | N/A |
| 5 | smallint, integer, bigint, numeric, real, double precision | Number | N/A |
| 6 | smallint\[\], integer\[\], bigint\[\], numeric\[\], real\[\], double precision\[\] | Number Set | N/A |
| 7 | text character varying(n)\[\], varchar(n)\[\], character(n)\[\], char(n) \[\], text\[\] | String Set | N/A |
| 8 | text character varying(n), varchar(n), character(n), char(n), text | String | N/A |

Identifier case handling
------------------------

PostgreSQL folds identifiers to lower case by default.
Rules and problems with DynamoDB identifiers **yet not tested and described**.

Generated columns
-----------------

Behaviour within generated columns **yet not tested and described**. 

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)


Character set handling
----------------------

**Yet not described**.

Examples
--------

### Install the extension:

Once for a database you need, as PostgreSQL superuser.

```sql
	CREATE EXTENSION dynamodb_fdw;
```
### Create a foreign server with appropriate configuration:

Once for a foreign datasource you need, as PostgreSQL superuser.

```sql
	CREATE SERVER dynamodb_svr
	FOREIGN DATA WRAPPER dynamodb_fdw
	OPTIONS (
	  endpoint 'http://localhost:8000'
	);
```

### Grant usage on foreign server to non-superuser in PostgreSQL:

Once for a non-superuser in PostgreSQL, as PostgreSQL superuser. It is a good idea to use a superuser only where really necessary, so let's allow a normal user to use the foreign server (this is not required for the example to work, but it's secirity recomedation).

```sql
	GRANT USAGE ON FOREIGN SERVER dynamodb_svr TO pguser;
```
Where `pgser` is a sample user for works with foreign server (and foreign tables).

### Create an appropriate user mapping:

```sql
	CREATE USER MAPPING
	FOR pgser
	SERVER dynamodb_svr 
    	OPTIONS(
	  username 'username',
	  password 'password'
	);
```
Where `pgser` is a sample user for works with foreign server (and foreign tables).

### Create a foreign table referencing the dynamodb table:
```sql
	CREATE FOREIGN TABLE frtbl (
	  c1 int,
	  c2 text,
	  c3 jsonb
	)
	SERVER dynamodb_svr
	OPTIONS (
	  table_name 'table1'
	);
```

### Query the foreign table.

```sql
	SELECT *
	FROM frtbl;
```	

Limitations
-----------

* Does not support `List` type of DynamoDB.
* Only support `SELECT` the `Binary` type of DynamoDB. Does not support `WHERE` clause, `INSERT`, `UPDATE` statement with `Binary` type of DynamoDB.
* For DynamoDB, 2 records can have the same attribute name but different data type. However, DynamoDB FDW does not support that case. User need to avoid using that case.
* Does not support `COPY FROM` and foreign partition routing. The following error will be shown.
    `COPY and foreign partition routing not supported in dynamodb_fdw`
    
Contributing
------------
Opening issues and pull requests on GitHub are welcome.

Useful links
------------

### Source code

Reference FDW realisation, `postgres_fdw`
 - https://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=contrib/postgres_fdw;hb=HEAD

### General FDW Documentation

 - https://www.postgresql.org/docs/current/ddl-foreign-data.html
 - https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
 - https://www.postgresql.org/docs/current/sql-createforeigntable.html
 - https://www.postgresql.org/docs/current/sql-importforeignschema.html
 - https://www.postgresql.org/docs/current/fdwhandler.html
 - https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

 - https://wiki.postgresql.org/wiki/Fdw
 - https://pgxn.org/tag/fdw/
 
License
-------
Copyright and license information can be found in the file [`LICENSE`][1].

[1]: LICENSE

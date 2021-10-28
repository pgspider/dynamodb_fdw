/*-------------------------------------------------------------------------
 *
 * dynamodb_query.hpp
 * 		Header file for type handling for DynamoDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		contrib/dynamodb_fdw/dynamodb_query.hpp
 *
 *-------------------------------------------------------------------------
 */
extern "C"
{
#include "postgres.h"
#include "utils/builtins.h"
}

#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>

Datum
dynamodb_convert_to_pg(Oid pgtyp, int pgtypmod, Aws::DynamoDB::Model::AttributeValue dynamodbVal);

Aws::DynamoDB::Model::AttributeValue
dynamodb_bind_sql_var(Oid type, int attnum, Datum value, const char * query, bool isnull);
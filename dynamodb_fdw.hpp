/*-------------------------------------------------------------------------
 *
 * dynamodb_fdw.hpp
 *		  Header file of dynamodb_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/dynamodb_fdw.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __DYNAMODB_FDW_HPP__
#define  __DYNAMODB_FDW_HPP__

#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>

extern "C"
{
#include "postgres.h"
#include "foreign/foreign.h"
#include "dynamodb_fdw.h"
}

#define IS_KEY_EMPTY(key) \
	(key == NULL || ((const Aws::String) key).size() == 0)

extern Aws::DynamoDB::DynamoDBClient *dynamodbGetConnection(UserMapping *user);
extern void dynamodb_report_error(int elevel, const Aws::String message, char* query);
extern void dynamodbReleaseConnection(Aws::DynamoDB::DynamoDBClient *dynamoDB_client);

#endif /* __DYNAMODB_FDW_HPP__ */
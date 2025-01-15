/*-------------------------------------------------------------------------
 *
 * dynamodb_query.cpp
 * 		Type handling for DynamoDB Foreign Data Wrapper
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		contrib/dynamodb_fdw/dynamodb_query.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "dynamodb_query.hpp"

#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/core/utils/memory/stl/AWSVector.h>

extern "C"
{
#include "postgres.h"

#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "dynamodb_fdw.h"
#include "jansson.h"
#include "string.h"

#include "utils/json.h"
#include "utils/jsonfuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
}

static void dynamodb_get_datatype_for_conversion(Oid pg_type, regproc *typeinput,
								   				int *typemod);
static ArrayType *dynamodb_convert_set_to_array(const Aws::Vector<Aws::String>, regproc *typeinput,
												int *typemod, Oid pgtyp);
static void dynamodb_convert_nested_object_to_json_string(StringInfo output,
												Aws::DynamoDB::Model::AttributeValue dynamodbVal);
static char *dynamodb_convert_bytebuf_to_string(Aws::Utils::ByteBuffer bytebuf);
static Aws::Utils::ByteBuffer dynamodb_convert_string_to_bytebuf(const char* str);
static char *dynamodb_convert_byte_datum_to_string(Datum value);

/*
 * dynamodb_convert_set_to_array
 *
 * Convert SET data type of DynamoDB to Array type of Postgres
 */
static ArrayType *
dynamodb_convert_set_to_array(const Aws::Vector<Aws::String> val,
					regproc *typeinput, int *typemod, Oid pgtyp)
{
	Datum	   *datumArr;
	size_t		size = val.size();
	int			i = 0;
	Datum		valueDatum = 0;
	Oid			elmtype;
	int			elmlen;
	bool		elmbyval;
	char		elmalign;

	datumArr = (Datum *) palloc0(sizeof(Datum) * size);
	switch (pgtyp)
	{
		case INT2ARRAYOID:
			{
				elmtype = INT2OID;
				elmlen = sizeof(int16_t);
				elmbyval = true;
				elmalign = 's';
				break;
			}
		case INT4ARRAYOID:
			{
				elmtype = INT4OID;
				elmlen = sizeof(int32_t);
				elmbyval = true;
				elmalign = 'i';
				break;
			}
		case INT8ARRAYOID:
			{
				elmtype = INT8OID;
				elmlen = sizeof(int64_t);
				elmbyval = true;
				elmalign = 'd';
				break;
			}
		case FLOAT4ARRAYOID:
			{
				elmtype = FLOAT4OID;
				elmlen = sizeof(float4);
				elmbyval = true;
				elmalign = TYPALIGN_INT;
				break;
			}
		case FLOAT8ARRAYOID:
			{
				elmtype = FLOAT8OID;
				elmlen = sizeof(float8);
				elmbyval = true;
				elmalign = 'd';
				break;
			}
		case NUMERICARRAYOID:
			{
				elmtype = NUMERICOID;
				elmlen = sizeof(float8);
				elmbyval = true;
				elmalign = 'd';
				break;
			}
		case BYTEAARRAYOID:
			{
				elmtype = BYTEAOID;
				elmlen = -1;
				elmbyval = false;
				elmalign = 'i';
				break;
			}
		default:
			{
				elmtype = TEXTOID;
				elmlen = -1;
				elmbyval = false;
				elmalign = 'i';
				break;
			}
	}

	dynamodb_get_datatype_for_conversion(elmtype, typeinput, typemod);

	for (const auto &item : val)
	{
		char *var_str = pstrdup(item.c_str());
		valueDatum = CStringGetDatum(var_str);
		datumArr[i] = OidFunctionCall3(*typeinput, valueDatum,
											ObjectIdGetDatum(InvalidOid),
											Int32GetDatum(*typemod));
		i++;
	}
	return construct_array(datumArr, size, elmtype,
							elmlen, elmbyval, elmalign);
}

static void
dynamodb_append_string_literal_value(StringInfo output, const char *val)
{
	const char *valptr;

	appendStringInfoChar(output, '\"');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if ((ch) == '\"')
			appendStringInfoChar(output, '\\');
		else if ( (ch) == '\\')
			appendStringInfoChar(output, ch);
		appendStringInfoChar(output, ch);
	}
	appendStringInfoChar(output, '\"');
}

/*
 * dynamodb_convert_nested_object_to_json_string
 *
 * Recursive function to convert nested objects of DynamoDB to JSON string
 */
static void
dynamodb_convert_nested_object_to_json_string(StringInfo output,
									Aws::DynamoDB::Model::AttributeValue dynamodbVal)
{
	switch (dynamodbVal.GetType())
	{
		case Aws::DynamoDB::Model::ValueType::NUMBER:
			{
				appendStringInfo(output, "%s", dynamodbVal.GetN().c_str());
				break;
			}
		case Aws::DynamoDB::Model::ValueType::STRING:
			{
				dynamodb_append_string_literal_value(output, dynamodbVal.GetS().c_str());
				break;
			}
		case Aws::DynamoDB::Model::ValueType::BOOL:
			{
				appendStringInfo(output, "%s", dynamodbVal.GetBool()?"true":"false");
				break;
			}
		case Aws::DynamoDB::Model::ValueType::NUMBER_SET:
			{
				const Aws::Vector<Aws::String> val = dynamodbVal.GetNS();
				bool	first = true;

				appendStringInfoChar(output, '[');

				for (const auto &item : val)
				{
					if (!first)
						appendStringInfoChar(output, ',');

					appendStringInfo(output, "%s", item.c_str());
					first = false;
				}

				appendStringInfoChar(output, ']');
				break;
			}
		case Aws::DynamoDB::Model::ValueType::STRING_SET:
			{
				const Aws::Vector<Aws::String> val = dynamodbVal.GetSS();
				bool	first = true;

				appendStringInfoChar(output, '[');

				for (const auto &item : val)
				{
					if (!first)
						appendStringInfoChar(output, ',');

					dynamodb_append_string_literal_value(output, item.c_str());
					first = false;
				}

				appendStringInfoChar(output, ']');
				break;
			}
		case Aws::DynamoDB::Model::ValueType::ATTRIBUTE_LIST:
			{
				const auto &listVal = dynamodbVal.GetL();
				bool first = true;

				appendStringInfoChar(output, '[');

				for (const auto& item : listVal)
				{
					if (!first)
						appendStringInfoChar(output, ',');

					dynamodb_convert_nested_object_to_json_string(output, *item);

					first = false;
				}

				appendStringInfoChar(output, ']');
				break;
			}
		case Aws::DynamoDB::Model::ValueType::NULLVALUE:
			{
				appendStringInfo(output, "null");
				break;
			}
		case Aws::DynamoDB::Model::ValueType::ATTRIBUTE_MAP:
			{
				const auto &mapVal = dynamodbVal.GetM();
				bool first = true;

				appendStringInfoChar(output, '{');

				for (const auto& item : mapVal)
				{
					if (!first)
						appendStringInfoChar(output, ',');

					appendStringInfo(output, "\"%s\":", item.first.c_str());
					dynamodb_convert_nested_object_to_json_string(output, *item.second);

					first = false;
				}

				appendStringInfoChar(output, '}');
				break;
			}
		case Aws::DynamoDB::Model::ValueType::BYTEBUFFER:
			{
				char 	*outputString = NULL;
				Oid 	outputFunctionId = InvalidOid;
				bool 	typeVarLength = false;
				const Aws::Utils::ByteBuffer bytebuf = dynamodbVal.GetB();
				size_t 	size = bytebuf.GetLength();
				Datum 	valueDatum = (Datum)palloc0(size + VARHDRSZ);

				memcpy(VARDATA(valueDatum), (void *)bytebuf.GetUnderlyingData(), size);
				SET_VARSIZE(valueDatum, size + VARHDRSZ);

				getTypeOutputInfo(BYTEAOID, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, valueDatum);

				dynamodb_append_string_literal_value(output, outputString);
				break;
			}
		case Aws::DynamoDB::Model::ValueType::BYTEBUFFER_SET:
			{
				const Aws::Vector<Aws::Utils::ByteBuffer> bytebuff = dynamodbVal.GetBS();
				bool 	first = true;

				appendStringInfoChar(output, '[');

				for (const auto &item : bytebuff)
				{
					char 	*outputString = NULL;
					Oid 	outputFunctionId = InvalidOid;
					bool 	typeVarLength = false;
					size_t 	size = item.GetLength();
					Datum 	valueDatum = (Datum)palloc0(size + VARHDRSZ);

					if (!first)
						appendStringInfoChar(output, ',');

					memcpy(VARDATA(valueDatum), (void *)item.GetUnderlyingData(), size);
					SET_VARSIZE(valueDatum, size + VARHDRSZ);

					getTypeOutputInfo(BYTEAOID, &outputFunctionId, &typeVarLength);
					outputString = OidOutputFunctionCall(outputFunctionId, valueDatum);

					dynamodb_append_string_literal_value(output, outputString);
					first = false;
				}

				appendStringInfoChar(output, ']');
				break;
			}
	}
}

/*
 * Search system cache and get types for output function.
 * They are used for data type conversion.
 */
static void
dynamodb_get_datatype_for_conversion(Oid pg_type, regproc *typeinput,
								   int *typemod)
{
	HeapTuple	hptuple;

	hptuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
	if (!HeapTupleIsValid(hptuple))
		elog(ERROR, "dynamodb_fdw: cache lookup failed for type%u", pg_type);

	*typeinput = ((Form_pg_type) GETSTRUCT(hptuple))->typinput;
	*typemod = ((Form_pg_type) GETSTRUCT(hptuple))->typtypmod;
	ReleaseSysCache(hptuple);
}

static bool
is_compatible_type(Oid pg_type, Aws::DynamoDB::Model::ValueType dynamodb_type)
{
	switch (dynamodb_type)
	{
		case Aws::DynamoDB::Model::ValueType::BOOL:
			{
				if (pg_type == BOOLOID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::BYTEBUFFER:
			{
				if (pg_type == BYTEAOID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::NUMBER:
			{
				if (pg_type == INT2OID ||
					pg_type == INT4OID ||
					pg_type == INT8OID ||
					pg_type == NUMERICOID ||
					pg_type == FLOAT4OID ||
					pg_type == FLOAT8OID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::STRING:
			{
				if (pg_type == TEXTOID ||
					pg_type == VARCHAROID ||
					pg_type == BPCHAROID ||
					pg_type == NAMEOID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::NUMBER_SET:
			{
				if (pg_type == INT2ARRAYOID ||
					pg_type == INT4ARRAYOID ||
					pg_type == INT8ARRAYOID ||
					pg_type == NUMERICARRAYOID ||
					pg_type == FLOAT4ARRAYOID ||
					pg_type == FLOAT8ARRAYOID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::STRING_SET:
			{
				if (pg_type == TEXTARRAYOID ||
					pg_type == VARCHARARRAYOID ||
					pg_type == BPCHARARRAYOID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::ATTRIBUTE_MAP:
		case Aws::DynamoDB::Model::ValueType::ATTRIBUTE_LIST:
			{
				if (pg_type == JSONBOID ||
					pg_type == JSONOID)
					return true;

				break;
			}
		case Aws::DynamoDB::Model::ValueType::BYTEBUFFER_SET:
			{
				if (pg_type == BYTEAARRAYOID)
					return true;

				break;
			}
		default:
			return false;
	}

	return false;
}

/*
 * dynamodb_convert_to_pg: Convert DynamoDB data into PostgreSQL's compatible data types
 */
Datum
dynamodb_convert_to_pg(Oid pgtyp, int pgtypmod, Aws::DynamoDB::Model::AttributeValue dynamodbVal)
{
	Datum		valueDatum = 0;
	Datum		returnDatum = 0;
	regproc		typeinput;
	int			typemod;

	switch (dynamodbVal.GetType())
	{
		case Aws::DynamoDB::Model::ValueType::BOOL:
			{
				bool val = dynamodbVal.GetBool();

				if (is_compatible_type(pgtyp, dynamodbVal.GetType()))
					return BoolGetDatum(val);
				else
				{
					StringInfo	buffer = makeStringInfo();

					dynamodb_convert_nested_object_to_json_string(buffer, dynamodbVal);
					valueDatum = CStringGetDatum(buffer->data);
				}

				break;
			}
		case Aws::DynamoDB::Model::ValueType::BYTEBUFFER:
			{
				if (is_compatible_type(pgtyp, dynamodbVal.GetType()))
				{
					const Aws::Utils::ByteBuffer bytebuf = dynamodbVal.GetB();

					size_t 	size = bytebuf.GetLength();
					valueDatum = (Datum)palloc0(size + VARHDRSZ);
					memcpy(VARDATA(valueDatum), (void *)bytebuf.GetUnderlyingData(), size);
					SET_VARSIZE(valueDatum, size + VARHDRSZ);
					return valueDatum;
				}
				else
				{
					StringInfo buffer = makeStringInfo();

					dynamodb_convert_nested_object_to_json_string(buffer, dynamodbVal);
					valueDatum = CStringGetDatum(buffer->data);
				}
				break;
			}
		case Aws::DynamoDB::Model::ValueType::NUMBER:
			{
				if (is_compatible_type(pgtyp, dynamodbVal.GetType()))
				{
					char *val = pstrdup(dynamodbVal.GetN().c_str());
					valueDatum = CStringGetDatum(val);
				}
				else
				{
					StringInfo	buffer = makeStringInfo();

					dynamodb_convert_nested_object_to_json_string(buffer, dynamodbVal);
					valueDatum = CStringGetDatum(buffer->data);
				}
				break;
			}
		case Aws::DynamoDB::Model::ValueType::STRING:
			{
				if (is_compatible_type(pgtyp, dynamodbVal.GetType()))
				{
					char *val = pstrdup(dynamodbVal.GetS().c_str());

					valueDatum = CStringGetDatum(val);
				}
				else
				{
					StringInfo	buffer = makeStringInfo();

					dynamodb_convert_nested_object_to_json_string(buffer, dynamodbVal);
					valueDatum = CStringGetDatum(buffer->data);
				}
				break;
			}
		case Aws::DynamoDB::Model::ValueType::NUMBER_SET:
			{
				const Aws::Vector<Aws::String> val = dynamodbVal.GetNS();
				ArrayType  *arr;

				if (is_compatible_type(pgtyp, dynamodbVal.GetType()))
				{
					arr = dynamodb_convert_set_to_array(val, &typeinput, &typemod, pgtyp);
					return PointerGetDatum(arr);
				}
				else
				{
					StringInfo	buffer = makeStringInfo();

					dynamodb_convert_nested_object_to_json_string(buffer, dynamodbVal);
					valueDatum = CStringGetDatum(buffer->data);
					break;
				}
			}
		case Aws::DynamoDB::Model::ValueType::STRING_SET:
			{
				const Aws::Vector<Aws::String> val = dynamodbVal.GetSS();
				ArrayType  *arr;

				arr = dynamodb_convert_set_to_array(val, &typeinput, &typemod, pgtyp);
				return PointerGetDatum(arr);
			}
		case Aws::DynamoDB::Model::ValueType::ATTRIBUTE_MAP:
		case Aws::DynamoDB::Model::ValueType::ATTRIBUTE_LIST:
			{
#if PG_VERSION_NUM >= 170000
				JsonLexContext lex;
#else
				JsonLexContext *lex;
#endif
				text	   *result;
				StringInfo	buffer = makeStringInfo();

				dynamodb_convert_nested_object_to_json_string(buffer, dynamodbVal);

				if (pgtyp == JSONOID)
				{
					result = cstring_to_text_with_len(buffer->data, buffer->len);
#if PG_VERSION_NUM >= 170000
					makeJsonLexContext(&lex, result, false);
					pg_parse_json(&lex, &nullSemAction);
					freeJsonLexContext(&lex);
#else
					lex = makeJsonLexContext(result, false);
					pg_parse_json(lex, &nullSemAction);
#endif
					return PointerGetDatum(result);
				}
				else if (pgtyp == JSONBOID)
					return DirectFunctionCall1(jsonb_in, PointerGetDatum(buffer->data));
				else
					valueDatum = CStringGetDatum(buffer->data);

				break;
			}
		case Aws::DynamoDB::Model::ValueType::BYTEBUFFER_SET:
			{
				const Aws::Vector<Aws::Utils::ByteBuffer> bytebuff = dynamodbVal.GetBS();
				Aws::Vector<Aws::String> val;
				ArrayType *arr;

				for (const auto &item : bytebuff)
				{
					char *buff = dynamodb_convert_bytebuf_to_string(item);
					val.push_back(buff);
				}

				arr = dynamodb_convert_set_to_array(val, &typeinput, &typemod, BYTEAARRAYOID);
				return PointerGetDatum(arr);
			}
		default:
			{
				/* Should not happen */
				elog(ERROR, "dynamodb_fdw: unsupported data type %d of DynamoDB", (int) dynamodbVal.GetType());
			}
	}

	dynamodb_get_datatype_for_conversion(pgtyp, &typeinput, &typemod);
	returnDatum = OidFunctionCall3(typeinput, valueDatum,
								   ObjectIdGetDatum(pgtyp),
								   Int32GetDatum(typemod));

	return returnDatum;
}

static Aws::DynamoDB::Model::AttributeValue
dynamodb_bind_json_value(json_t *root, char* key_name)
{
	Aws::DynamoDB::Model::AttributeValue bindValue;

	switch(root->type)
		{
			case JSON_STRING:
			{
				bindValue.SetS(json_string_value(root));
				break;
			}
			case JSON_INTEGER:
			{
				bindValue.SetN((int) json_integer_value(root));
				break;
			}
			case JSON_REAL:
			{
				bindValue.SetN(json_real_value(root));
				break;
			}
			case JSON_TRUE:
			{
				bindValue.SetBool(true);
				break;
			}
			case JSON_FALSE:
			{
				bindValue.SetBool(false);
				break;
			}
			case JSON_NULL:
			{
				bindValue.SetNull(true);
				break;
			}
			case JSON_ARRAY:
			{
				Aws::Vector<std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> listAtt;
				unsigned int	i;
				json_t *value;

				json_array_foreach(root, i, value)
				{
					Aws::DynamoDB::Model::AttributeValue attVal;

					attVal = dynamodb_bind_json_value(value, NULL);
					listAtt.push_back(std::make_shared<Aws::DynamoDB::Model::AttributeValue>(attVal));
				}
				bindValue.SetL(listAtt);
				break;
			}
			case JSON_OBJECT:
			{
				Aws::Map<Aws::String, const std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> mapAtt;
				const char *key = NULL;
				json_t *element = NULL;

				json_object_foreach(root, key, element)
				{
					Aws::DynamoDB::Model::AttributeValue attVal;

					attVal = dynamodb_bind_json_value(element, NULL);
					mapAtt.insert(mapAtt.begin(), std::pair<Aws::String,
								const std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>>
								(key, std::make_shared<Aws::DynamoDB::Model::AttributeValue>(attVal)));
				}
				bindValue.SetM(mapAtt);
				break;
			}
		}
	return bindValue;
}

void
dynamodb_bind_array(Oid element_type, Datum value, Aws::Vector<Aws::String> *vectorValues)
{
	ArrayType  *arr;
	Datum      *values;
	bool       *nulls;
	int         num;
	char	   *outputString = NULL;
	Oid			outputFunctionId = InvalidOid;
	bool		typeVarLength = false;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;

	arr = DatumGetArrayTypeP(value);
	get_typlenbyvalalign(ARR_ELEMTYPE(arr),
						&elmlen, &elmbyval, &elmalign);

	deconstruct_array(arr, element_type, elmlen, elmbyval, elmalign, &values, &nulls, &num);

	if (num == 0)
		elog(ERROR, "DynamoDB does not support empty set");

	for (int i = 0; i < num; i++)
	{
		if (element_type == TEXTOID)
		{
			if (values[i] == 0)
				outputString = (char *) "";
			else
				outputString = TextDatumGetCString(values[i]);
		}
		else if (element_type == BYTEAOID)
		{
			outputString = dynamodb_convert_byte_datum_to_string(values[i]);
		}
		else
		{
			getTypeOutputInfo(element_type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, values[i]);
		}

		vectorValues->push_back(outputString);
	}
}

/*
 * bind_sql_var
 *
 * Bind the values provided as Datum and nulls to modify the target table (INSERT/UPDATE)
 * 
 */
Aws::DynamoDB::Model::AttributeValue
dynamodb_bind_sql_var(Oid type, int attnum, Datum value, const char *query, bool isnull)
{
	Aws::DynamoDB::Model::AttributeValue value1;
	
	if (isnull)
	{
		value1.SetNull(true);
		return value1;
	}

	switch (type)
	{
	 	case INT2OID:
	 		{
	 			int16		dat = DatumGetInt16(value);
	 			value1.SetN(dat);
	 			break;
	 		}
	 	case INT4OID:
	 		{
	 			int32		dat = DatumGetInt32(value);
	 			value1.SetN(dat);
	 			break;
	 		}
	 	case INT8OID:
	 		{
	 			int32		dat = DatumGetInt32(value);
				value1.SetN(dat);
	 			break;
	 		}

	 	case FLOAT4OID:
	 		{
	 			float4		dat = DatumGetFloat4(value);
				value1.SetN(dat);
	 			break;
	 		}
	 	case FLOAT8OID:
	 		{
	 			float8		dat = DatumGetFloat8(value);
				value1.SetN(dat);
	 			break;
	 		}

	 	case NUMERICOID:
	 		{
	 			Datum		valueDatum = DirectFunctionCall1(numeric_float8, value);
	 			float8		dat = DatumGetFloat8(valueDatum);
				value1.SetN(dat);

	 			break;
	 		}
	 	case BOOLOID:
	 		{
	 			int32		dat = DatumGetInt32(value);
				value1.SetBool(dat);
	 			break;
	 		}
		case NUMERICARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(NUMERICOID, value, &vectorValues);
				value1.SetNS(vectorValues);
				break;
			}
		case FLOAT4ARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(FLOAT4OID, value, &vectorValues);
				value1.SetNS(vectorValues);
				break;
			}
		case FLOAT8ARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(FLOAT8OID, value, &vectorValues);
				value1.SetNS(vectorValues);
				break;
			}
		case INT2ARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(INT2OID, value, &vectorValues);
				value1.SetNS(vectorValues);
				break;
			}
		case INT4ARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(INT4OID, value, &vectorValues);
				value1.SetNS(vectorValues);
				break;
			}
		case INT8ARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(INT8OID, value, &vectorValues);
				value1.SetNS(vectorValues);
				break;
			}
		case TEXTARRAYOID:
		case VARCHARARRAYOID:
		case BPCHARARRAYOID:
			{	
				Aws::Vector<Aws::String> vectorValues;

				dynamodb_bind_array(TEXTOID, value, &vectorValues);
				value1.SetSS(vectorValues);
				break;
			}
	 	case TEXTOID:
	 	case VARCHAROID:
	 	case BPCHAROID:
	 		{
	 			char	   *outputString = NULL;
	 			Oid			outputFunctionId = InvalidOid;
	 			bool		typeVarLength = false;

	 			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
	 			outputString = OidOutputFunctionCall(outputFunctionId, value);
	 			value1.SetS(outputString);
	 			break;
	 		}
		case JSONOID:
		case JSONBOID:
			{
				char		   *outputString = NULL;
				Oid				outputFunctionId = InvalidOid;
				bool			typeVarLength = false;
				json_t		   *root;
				json_error_t	error;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);

				root = json_loads(outputString, JSON_DECODE_ANY, &error);

				value1 = dynamodb_bind_json_value(root, NULL);
				break;
			}
			case BYTEAOID:
			{
				Aws::Utils::ByteBuffer bytebuf;
				char 	*bufptr = dynamodb_convert_byte_datum_to_string(value);

				/* Convert string to bytebuf */
				bytebuf = dynamodb_convert_string_to_bytebuf(bufptr);

				value1.SetB(bytebuf);
				break;
			}
			case BYTEAARRAYOID:
			{
				Aws::Vector<Aws::String> vectorValues;
				Aws::Vector<Aws::Utils::ByteBuffer> vectorBinaryValues;

				dynamodb_bind_array(BYTEAOID, value, &vectorValues);

				/* Convert string vector to byte buffer vector */
				for (const auto &item : vectorValues)
				{
					Aws::Utils::ByteBuffer bytebuf = dynamodb_convert_string_to_bytebuf(item.c_str());
					vectorBinaryValues.push_back(bytebuf);
				}

				value1.SetBS(vectorBinaryValues);
				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("dynamodb_fdw: cannot convert constant value to DynamoDB value %u", type),
								errhint("Constant value data type: %u", type)));
	 			break;
	 		}
	 	}
	return value1;
}

/*
 * dynamodb_convert_bytebuf_to_string
 *
 * Convert a byte buffer to a string
 *
 */
static char *dynamodb_convert_bytebuf_to_string(Aws::Utils::ByteBuffer bytebuf)
{
	size_t 	size = bytebuf.GetLength();
	char 	*buff = (char *)palloc0(size + 1);

	/* Convert unsigned char array to char array */
	for (size_t i = 0; i < size; i++)
	{
		buff[i] = bytebuf.GetItem(i);
	}
	buff[size] = 0;

	return buff;
}

/*
 * dynamodb_convert_string_to_bytebuf
 *
 * Convert a string to byte buffer
 *
 */
static Aws::Utils::ByteBuffer dynamodb_convert_string_to_bytebuf(const char* str)
{
	size_t 	size = strlen(str);
	unsigned char 	*uchar = (unsigned char*) palloc0(size + 1);

	/* Convert char array to unsigned char array */
	for (size_t i = 0; i < size; i++)
	{
		uchar[i] = str[i];
	}
	uchar[size] = 0;

	return Aws::Utils::ByteBuffer(uchar, size);
}

/*
 * dynamodb_convert_byte_datum_to_string
 *
 * Convert a bytea datum to string
 *
 */
static char *dynamodb_convert_byte_datum_to_string(Datum value)
{
	int 	len;
	char 	*dat = NULL;
	char 	*bufptr;
	char 	*result = DatumGetPointer(value);

	if (VARATT_IS_1B(result))
	{
		len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
		dat = VARDATA_1B(result);
	}
	else
	{
		len = VARSIZE_4B(result) - VARHDRSZ;
		dat = VARDATA_4B(result);
	}

	bufptr = (char *)palloc0(len + 1);
	memcpy(bufptr, (char *)dat, len);

	return bufptr;
}

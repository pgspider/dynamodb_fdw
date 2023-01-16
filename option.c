/*-------------------------------------------------------------------------
 *
 * option.c
 *		  FDW option handling for dynamodb_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "dynamodb_fdw.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "utils/lsyscache.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct DynamodbFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
/*
 * Valid options for dynamodb_fdw.
 */

static struct DynamodbFdwOption valid_options[] =
{
	/* Connection options */
	{"endpoint", ForeignServerRelationId},
	{"partition_key", ForeignTableRelationId},
	{"sort_key", ForeignTableRelationId},
	{"user", UserMappingRelationId},
	{"password", UserMappingRelationId},
	{"table_name", ForeignTableRelationId},
	{"column_name", AttributeRelationId},
	/* Sentinel */
	{NULL, InvalidOid}
};

extern Datum dynamodb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dynamodb_fdw_validator);
/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses dynamodb_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
dynamodb_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;
	/*
	 * Check that only options supported by dynamodb_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			struct DynamodbFdwOption *opt;
			StringInfoData buf;

			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("dynamodb_fdw: invalid option \"%s\"", def->defname),
					 buf.len > 0
					 ? errhint("Valid options in this context are: %s",
							   buf.data)
					 : errhint("There are no valid options in this context.")));
		}
	}
	PG_RETURN_VOID();
}
/*
 * Check whether the given option is one of the valid dynamodb_fdw options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool is_valid_option(const char *option, Oid context)
{
	struct DynamodbFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}
/*
 * Fetch the options for a dynamodb_fdw foreign table.
 */
dynamodb_opt *dynamodb_get_options(Oid foreignoid)
{
	UserMapping *f_mapping;
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	List	   *options;
	ListCell   *lc;
	dynamodb_opt *opt;

	opt = (dynamodb_opt *) palloc0(sizeof(dynamodb_opt));

	/*
	 * Extract options from FDW objects.
	 */
	PG_TRY();
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	PG_CATCH();
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}
	PG_END_TRY();

	options = NIL;
	if (f_table)
		options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);

	f_mapping = GetUserMapping(GetUserId(), f_server->serverid);
	options = list_concat(options, f_mapping->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "endpoint") == 0)
			opt->svr_endpoint = defGetString(def);

		if (strcmp(def->defname, "user") == 0)
			opt->svr_username = defGetString(def);

		if (strcmp(def->defname, "password") == 0)
			opt->svr_password = defGetString(def);
		
		if (strcmp(def->defname, "partition_key") == 0)
			opt->svr_partition_key = defGetString(def);
		
		if (strcmp(def->defname, "sort_key") == 0)
			opt->svr_sort_key = defGetString(def);
	}

	/* Default values, if required */
	if (!opt->svr_endpoint)
		opt->svr_endpoint = "http://localhost:8000";

	return opt;
}

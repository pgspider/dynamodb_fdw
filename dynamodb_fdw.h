/*-------------------------------------------------------------------------
 *
 * dynamodb_fdw.h
 *		  Foreign-data wrapper for DynamoDB
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/dynamodb_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DYNAMODB_FDW_H
#define DYNAMODB_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/pathnodes.h"
#include "utils/relcache.h"
#include "catalog/pg_operator.h"
#define CODE_VERSION 10400

/*
 * Options structure to store the dynamodb
 * server information
 */
typedef struct dynamodb_opt
{
	char	   *svr_endpoint;		/* dynamodb server ip address */
	char	   *svr_username;		/* dynamodb user name */
	char	   *svr_password;		/* dynamodb password */
	char	   *svr_partition_key;	/* dynamodb partition_key */
	char	   *svr_sort_key;		/* dynamodb sort_key */
} dynamodb_opt;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * postgres_fdw foreign table.  For a baserel, this struct is created by
 * postgresGetForeignRelSize, although some fields are not filled till later.
 * postgresGetForeignJoinPaths creates it for a joinrel, and
 * postgresGetForeignUpperPaths creates it for an upperrel.
 */
typedef struct DynamoDBFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 * All entries in these lists should have RestrictInfo wrappers; that
	 * improves efficiency of selectivity and cost estimation.
	 */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Estimated size and cost for a scan, join, or grouping/aggregation. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/*
	 * Estimated number of rows fetched from the foreign server, and costs
	 * excluding costs for transferring those rows from the foreign server.
	 * These are only used by estimate_path_cost_size().
	 */
	double		retrieved_rows;
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	List	   *shippable_extensions;	/* OIDs of whitelisted extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

} DynamoDBFdwRelationInfo;

typedef enum DynamoDBOperatorsSupport
{
	OP_CONDITIONAL = 1,
	OP_JSON,
	OP_UNSUPPORT,
} DynamoDBOperatorsSupport;

/* in dynamodb_impl.cpp */
extern int	dynamodb_set_transmission_modes(void);
extern void dynamodb_reset_transmission_modes(int nestlevel);

/* in option.c */
extern dynamodb_opt *dynamodb_get_options(Oid foreigntableid, Oid userid);

/* in deparse.cpp */
extern void dynamodb_classify_conditions(PlannerInfo *root,
				   RelOptInfo *baserel,
				   List *input_conds,
				   List **remote_conds,
				   List **local_conds);
extern bool dynamodb_is_foreign_expr(PlannerInfo *root,
							RelOptInfo *baserel,
							Expr *expr);
extern void dynamodb_deparse_insert(StringInfo buf, RangeTblEntry *rte,
				 		Index rtindex, Relation rel,
						List *targetAttrs, List **retrieved_attrs);
extern void dynamodb_deparse_delete(StringInfo buf, RangeTblEntry *rte,
				 		Index rtindex, Relation rel,
				 		List *returningList,
						List **retrieved_attrs, List *attnums);
extern void dynamodb_deparse_update(StringInfo buf, RangeTblEntry *rte,
							 Index rtindex, Relation rel,
							 List *targetAttrs,
							 List *withCheckOptionList, List *returningList,
							 List **retrieved_attrs, List *attnums);
extern void dynamodb_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root,
												RelOptInfo *foreignrel, List *tlist,
												List *remote_conds, List *pathkeys,
												List **retrieved_attrs);
extern bool dynamodb_tlist_has_json_arrow_op(PlannerInfo *root, RelOptInfo *baserel, List *tlist);
extern Form_pg_operator dynamodb_get_operator_expression(Oid oid);
extern DynamoDBOperatorsSupport dynamodb_validate_operator_name(Form_pg_operator opform);
extern void dynamodb_get_document_path(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, Expr *expr);
/* in shippable.c */
extern bool dynamodb_is_builtin(Oid objectId);
extern bool dynamodb_is_shippable(Oid objectId, Oid classId, DynamoDBFdwRelationInfo *fpinfo);

#endif							/* DYNAMODB_FDW_H */

/*-------------------------------------------------------------------------
 *
 * dynamodb_impl.cpp
 *		  Implementation for dynamodb_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/dynamodb_impl.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "dynamodb_fdw.hpp"
#include "dynamodb_query.hpp"
#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeValue.h>
#include <aws/dynamodb/model/ExecuteStatementRequest.h>

extern "C"
{
#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "nodes/print.h"
}

using namespace Aws::DynamoDB;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

#define DYNAMODB_ALLOCATION_TAG "DYNAMODB_ALLOCATION_TAG"

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth(). 	For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a postgres_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *        (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as a Boolean node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs
};

/* Struct for extra information passed to estimate_path_cost_size() */
typedef struct
{
	PathTarget *target;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
} DynamoDBFdwPathExtraData;

/*
 * Execution state of a foreign scan using dynamodb_fdw.
 */
typedef struct DynamoDBFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc	tupdesc;		/* tuple descriptor of scan */

	/* extracted fdw_private data */
	char	   *query;			/* text of SELECT command */
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */

	/* for remote query execution */
	Aws::DynamoDB::DynamoDBClient	   *conn;			/* connection for the scan */
	bool		cursor_exists;	/* have we created the cursor? */

	/* for storing result tuples */
	HeapTuple		tuples;			/* array of currently-retrieved tuples */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	bool		eof_reached;	/* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt;	/* context holding current batch of tuples */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* fetch more data */
	char	   	   *next_token;			/* next token for next request to retrieve more data */
	bool			next_fetch_ready;	/* true if DynamoDB FDW ready for next fetch */
	unsigned int	row_index;			/* the index of current processing item in the result set */
	bool			first_fetch;		/* true if the first time data is fetched */
	unsigned int	num_rows;			/* number of rows in data set */
	std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result;	/* contains the result of query */
} DynamoDBFdwScanState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct DynamoDBFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */

	/* for remote query execution */
	Aws::DynamoDB::DynamoDBClient   *conn;		/* connection for the scan */
	char	   *p_name;			/* name of prepared statement, if created */

	/* extracted fdw_private data */
	char	   *query;			/* text of INSERT/UPDATE/DELETE command */
	List	   *target_attrs;	/* list of target attribute numbers */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */

	/* for update row movement if subplan result rel */
	struct DynamoDBFdwModifyState *aux_fmstate;	/* foreign-insert state, if
											 * created */
	AttrNumber *junk_idx;		/* indexes of key columns */
} DynamoDBFdwModifyState;


/*
 * Helper functions
 */
static void dynamodb_estimate_path_cost_size(PlannerInfo *root,
									RelOptInfo *foreignrel,
									List *param_join_conds,
									List *pathkeys,
									DynamoDBFdwPathExtraData *fpextra,
									double *p_rows, int *p_width,
									Cost *p_startup_cost, Cost *p_total_cost);
static void create_cursor(ForeignScanState *node);
extern DynamoDBFdwModifyState *dynamodb_create_foreign_modify(EState *estate,
											   RangeTblEntry *rte,
											   ResultRelInfo *resultRelInfo,
											   CmdType operation,
											   Plan *subplan,
											   char *query,
											   List *target_attrs,
											   bool has_returning,
											   List *retrieved_attrs);
static void fetch_more_data(ForeignScanState *node);
static HeapTuple make_tuple_from_result_row(std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result,
											unsigned int *row_index,
											Relation rel,
											List *retrieved_attrs,
											ForeignScanState *fsstate,
											MemoryContext temp_context);
static void dynamodb_store_returning_result(DynamoDBFdwModifyState *fmstate,
											TupleTableSlot *slot,
											std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result);
static List *dynamodb_get_key_names(TupleDesc tupdesc, Oid foreignTableId, char *partition_key,
											char *sort_key);
/*
 * dynamodbGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
extern "C" void
dynamodbGetForeignRelSize(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	DynamoDBFdwRelationInfo *fpinfo;
	ListCell   *lc;

	/*
	 * We use DynamoDBFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (DynamoDBFdwRelationInfo *) palloc0(sizeof(DynamoDBFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.
	 */
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->shippable_extensions = NIL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	dynamodb_classify_conditions(root, baserel, baserel->baserestrictinfo,
					   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to dynamodb_estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the foreign table has never been ANALYZEd, it will have
	 * reltuples < 0, meaning "unknown".  We can't do much if we're not
	 * allowed to consult the remote server, but we can use a hack similar
	 * to plancat.c's treatment of empty relations: use a minimum size
	 * estimate of 10 pages, and divide by the column-datatype-based width
	 * estimate to get the corresponding number of tuples.
	 */
#if (PG_VERSION_NUM >= 140000)
	if (baserel->tuples < 0)
#else
	if (baserel->pages == 0 && baserel->tuples == 0)
#endif
	{
		baserel->pages = 10;
		baserel->tuples =
			(10 * BLCKSZ) / (baserel->reltarget->width +
								MAXALIGN(SizeofHeapTupleHeader));
	}

	/* Estimate baserel size as best we can with local statistics. */
	set_baserel_size_estimates(root, baserel);

	/* Fill in basically-bogus cost estimates for use later. */
	dynamodb_estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
							&fpinfo->rows, &fpinfo->width,
							&fpinfo->startup_cost, &fpinfo->total_cost);

}

/*
 * dynamodb_estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 * fpextra specifies additional post-scan/join-processing steps such as the
 * final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
dynamodb_estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						DynamoDBFdwPathExtraData *fpextra,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	DynamoDBFdwRelationInfo *fpinfo = (DynamoDBFdwRelationInfo *) foreignrel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		run_cost = 0;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	/*
	 * We don't support join conditions in this mode (hence, no
	 * parameterized paths can be made).
	 */
	Assert(param_join_conds == NIL);

	/*
	 * We will come here again and again with different set of pathkeys or
	 * additional post-scan/join-processing steps that caller wants to
	 * cost.  We don't need to calculate the cost/size estimates for the
	 * underlying scan, join, or grouping each time.  Instead, use those
	 * estimates if we have cached them already.
	 */
	if (fpinfo->rel_startup_cost >= 0 && fpinfo->rel_total_cost >= 0)
	{
#if PG_VERSION_NUM >= 140000
		Assert(fpinfo->retrieved_rows >= 0);
#else
		Assert(fpinfo->retrieved_rows >= 1);
#endif

		rows = fpinfo->rows;
		retrieved_rows = fpinfo->retrieved_rows;
		width = fpinfo->width;
		startup_cost = fpinfo->rel_startup_cost;
		run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
	}
	else
	{
		Cost		cpu_per_tuple;

		/* Use rows/width estimates made by set_baserel_size_estimates. */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/*
		 * Back into an estimate of the number of retrieved rows.  Just in
		 * case this is nuts, clamp to at most foreignrel->tuples.
		 */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
		retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

		/*
		 * Cost as though this were a seqscan, which is pessimistic.  We
		 * effectively imagine the local_conds are being evaluated
		 * remotely, too.
		 */
		startup_cost = 0;
		run_cost = 0;
		run_cost += seq_page_cost * foreignrel->pages;

		startup_cost += foreignrel->baserestrictcost.startup;
		cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
		run_cost += cpu_per_tuple * foreignrel->tuples;

		/* Add in tlist eval cost for each output row */
		startup_cost += foreignrel->reltarget->cost.startup;
		run_cost += foreignrel->reltarget->cost.per_tuple * rows;
	}

	/*
	 * Without remote estimates, we have no real way to estimate the cost
	 * of generating sorted output.  It could be free if the query plan
	 * the remote side would have chosen generates properly-sorted output
	 * anyway, but in most cases it will cost something.  Estimate a value
	 * high enough that we won't pick the sorted path when the ordering
	 * isn't locally useful, but low enough that we'll err on the side of
	 * pushing down the ORDER BY clause when it's useful to do so.
	 */
	if (pathkeys != NIL)
	{
		startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
	}

	total_cost = startup_cost + run_cost;

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL)
	{
		fpinfo->retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

/*
 * dynamodbGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
extern "C" void
dynamodbGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	DynamoDBFdwRelationInfo *fpinfo = (DynamoDBFdwRelationInfo *) baserel->fdw_private;
	ForeignPath *path;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 *
	 * Although this path uses no join clauses, it could still have required
	 * parameterization due to LATERAL refs in its tlist.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL,	/* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL, /* no pathkeys */
								   baserel->lateral_relids,
								   NULL,	/* no extra plan */
								   NIL);	/* no fdw_private list */
	add_path(baserel, (Path *) path);

}

/*
 * dynamodbGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
extern "C" ForeignScan *
dynamodbGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *foreignrel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
	DynamoDBFdwRelationInfo *fpinfo = (DynamoDBFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *fdw_scan_tlist = NIL;
	List	   *fdw_recheck_quals = NIL;
	List	   *retrieved_attrs = NIL;
	StringInfoData sql;
	bool		tlist_has_json_arrow_op;
	ListCell   *lc;

	/* DynamoDB FDW only support simple relation */
	Assert(IS_SIMPLE_REL(foreignrel));

	/* Decide to execute Json arrow operator support in the target list. */
	tlist_has_json_arrow_op = dynamodb_tlist_has_json_arrow_op(root, foreignrel, tlist);

	/*
	 * For base relations, set scan_relid as the relid of the relation.
	 */
	scan_relid = foreignrel->relid;

	/*
	 * In a base-relation scan, we must apply the given scan_clauses.
	 *
	 * Separate the scan_clauses into those that can be executed remotely
	 * and those that can't.  baserestrictinfo clauses that were
	 * previously determined to be safe or unsafe by classifyConditions
	 * are found in fpinfo->remote_conds and fpinfo->local_conds.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local
	 * execution.
	 */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (dynamodb_is_foreign_expr(root, foreignrel, rinfo->clause))
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	/*
	 * For a base-relation scan, we have to support EPQ recheck, which
	 * should recheck all the remote quals.
	 */
	fdw_recheck_quals = remote_exprs;

	/*
	 * Build the list of columns that contain Jsonb arrow operator
	 * to be fetched from the foreign server.
	 */
	if (tlist_has_json_arrow_op == true)
	{
		List	 *document_path_list = NIL;

		if (tlist)
			fdw_scan_tlist = list_copy(tlist);
		else
			fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, foreignrel->reltarget->exprs);

		foreach(lc, local_exprs)
		{
			Node *node = (Node *)lfirst(lc);

			fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
												pull_var_clause((Node *) node,
																PVC_RECURSE_PLACEHOLDERS));
		}
		foreach(lc, remote_exprs)
		{
			Node *node = (Node *)lfirst(lc);

			fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
												pull_var_clause((Node *) node,
																PVC_RECURSE_PLACEHOLDERS));
		}

		/*
		 * DynamoDB does not support overlap document path.
		 * For example: SELECT friends.login.lastsignin, friends.login, friends.
		 * Therefore, do not push down arrow operators in that case.
		 */
		foreach(lc, fdw_scan_tlist)
		{
			StringInfoData document_path;
			Expr *expr = ((TargetEntry *)lfirst(lc))->expr;
			ListCell *attlc;

			initStringInfo(&document_path);
			dynamodb_get_document_path(&document_path, root, foreignrel, expr);

			foreach(attlc, document_path_list)
			{
				char *target_name = strVal(lfirst(attlc));

				if (strstr(target_name, document_path.data) != NULL ||
					strstr(document_path.data, target_name) != NULL)
				{
					fdw_scan_tlist = NULL;
					tlist_has_json_arrow_op = false;
					break;
				}
			}
			document_path_list = lappend(document_path_list, makeString(document_path.data));
		}
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	dynamodb_deparse_select_stmt_for_rel(&sql, root, foreignrel, fdw_scan_tlist,
							remote_exprs, best_path->path.pathkeys,
							&retrieved_attrs);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make2(makeString(sql.data),
							 retrieved_attrs);

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							NULL,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);
}

#if PG_VERSION_NUM >= 140000
/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc
dynamodb_get_tupdesc_for_join_scan_tuples(ForeignScanState *node)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	TupleDesc	tupdesc;

	/*
	 * The core code has already set up a scan tuple slot based on
	 * fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough,
	 * but there's one case where it isn't.  If we have any whole-row row
	 * identifier Vars, they may have vartype RECORD, and we need to replace
	 * that with the associated table's actual composite type.  This ensures
	 * that when we read those ROW() expression values from the remote server,
	 * we can convert them to a composite type the local server knows.
	 */
	tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var		   *var;
		RangeTblEntry *rte;
		Oid			reltype;

		/* Nothing to do if it's not a generic RECORD attribute */
		if (att->atttypid != RECORDOID || att->atttypmod >= 0)
			continue;

		/*
		 * If we can't identify the referenced table, do nothing.  This'll
		 * likely lead to failure later, but perhaps we can muddle through.
		 */
		var = (Var *) list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
									i)->expr;
		if (!IsA(var, Var) || var->varattno != 0)
			continue;
		rte = (RangeTblEntry *) list_nth(estate->es_range_table, var->varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;
		reltype = get_rel_type_id(rte->relid);
		if (!OidIsValid(reltype))
			continue;
		att->atttypid = reltype;
		/* shouldn't need to change anything else */
	}
	return tupdesc;
}
#endif

/*
 * dynamodbBeginForeignScan
 *		Initiate an executor scan of a foreign DynamoDB table.
 */
extern "C" void
dynamodbBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	DynamoDBFdwScanState *fsstate;
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	UserMapping *user;
	int			rtindex;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	fsstate = (DynamoDBFdwScanState *) palloc0(sizeof(DynamoDBFdwScanState));
	node->fdw_state = (void *) fsstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = exec_rt_fetch(rtindex, estate);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	user = GetUserMapping(userid, table->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->conn = dynamodb_get_connection(user);

	/* Init data for cursor_exists as false */
	fsstate->cursor_exists = false;

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												 FdwScanPrivateRetrievedAttrs);

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "dynamodb_fdw tuple data",
											   ALLOCSET_DEFAULT_SIZES);
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "dynamodb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		fsstate->rel = node->ss.ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	}
	else
	{
		fsstate->rel = NULL;
#if (PG_VERSION_NUM >= 140000)
		fsstate->tupdesc = dynamodb_get_tupdesc_for_join_scan_tuples(node);
#else
		fsstate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
#endif
	}

}

/*
 * dynamodbIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
extern "C" TupleTableSlot *
dynamodbIterateForeignScan(ForeignScanState *node)
{
	DynamoDBFdwScanState *fsstate = (DynamoDBFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If this is the first call after Begin or ReScan, we need to create the
	 * cursor on the remote side.
	 */
	if (!fsstate->cursor_exists)
		create_cursor(node);

	fsstate->tuples = NULL;

	/* No point in another fetch if we already detected EOF, though. */
	if (!fsstate->eof_reached)
		fetch_more_data(node);
	/* If we didn't get any tuples, must be end of data. */
	if (fsstate->tuples == NULL)
		return ExecClearTuple(slot);

	/*
	 * Return the next tuple.
	 */
	ExecStoreHeapTuple(fsstate->tuples, slot, false);

	return slot;
}

/*
 * dynamodbReScanForeignScan
 *		Restart the scan.
 */
extern "C" void
dynamodbReScanForeignScan(ForeignScanState *node)
{
	DynamoDBFdwScanState *fsstate = (DynamoDBFdwScanState *) node->fdw_state;

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->cursor_exists)
		return;

	/* Now force a fresh FETCH. */
	fsstate->tuples = NULL;
	fsstate->eof_reached = false;
	fsstate->first_fetch = true;
	fsstate->next_token = NULL;
	fsstate->next_fetch_ready = true;
	fsstate->row_index = 0;
}


/*
 * dynamodbEndForeignScan
 * Finish scanning foreign table and dispose objects used for this scan
 */
extern "C" void
dynamodbEndForeignScan(ForeignScanState *node)
{
	DynamoDBFdwScanState *fsstate = (DynamoDBFdwScanState *) node->fdw_state;

	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Release remote connection */
	dynamodb_release_connection(fsstate->conn);
	fsstate->conn = NULL;

	/* MemoryContexts will be deleted automatically. */
	
}

extern "C" void
dynamodbAddForeignUpdateTargets(
#if (PG_VERSION_NUM >= 140000)
								PlannerInfo *root,
								Index rtindex,
#else
								Query *parsetree,
#endif
								RangeTblEntry *target_rte,
								Relation target_relation)
{
	Oid       relid = RelationGetRelid(target_relation);
	dynamodb_opt *opt;
	opt = dynamodb_get_options(relid);
	char *partition_key = opt->svr_partition_key;
	char *sort_key = opt -> svr_sort_key;
	TupleDesc	tupdesc = target_relation->rd_att;

	if (IS_KEY_EMPTY(partition_key))
		elog(ERROR, "dynamodb_fdw: The partition_key option has not been set");

	for (int i = 0; i < tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var		   *var;
		char *attrname = NameStr(att->attname);

		if (strcmp(partition_key, attrname) == 0 ||
			(!IS_KEY_EMPTY(sort_key) && strcmp(sort_key, attrname) == 0))
		{
#if PG_VERSION_NUM < 140000
			Index	rtindex = parsetree->resultRelation;
			TargetEntry *tle;
#endif
			/* Make a Var representing the desired value */
			var = makeVar(rtindex,
							att->attnum,
							att->atttypid,
							att->atttypmod,
							att->attcollation,
							0);
#if PG_VERSION_NUM >= 140000
			/* Register it as a row-identity column needed by this target rel */
			add_row_identity_var(root, var, rtindex, pstrdup(NameStr(att->attname)));
#else
			tle = makeTargetEntry((Expr *) var,
								list_length(parsetree->targetList) + 1,
								pstrdup(NameStr(att->attname)), true);

			/* ... and add it to the query's targetlist */
			parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
		}
	}
}

/*
 * dynamodbPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 */
extern "C" List *
dynamodbPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	StringInfoData sql;
	List	   *targetAttrs = NIL;
	List	   *withCheckOptionList = NIL; 
	List	   *returningList = NIL; 
	List	   *retrieved_attrs = NIL;
	bool		trigger_update = false;
	List	   *condAttr = NULL;
	Oid			foreignTableId;
	TupleDesc	tupdesc;
	dynamodb_opt   *opt;
	char		   *partition_key;
	char		   *sort_key;

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);
	foreignTableId = RelationGetRelid(rel);
	tupdesc = RelationGetDescr(rel);

	opt = dynamodb_get_options(foreignTableId);
	partition_key = opt->svr_partition_key;
	sort_key = opt -> svr_sort_key;

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
	 * foreign table, we transmit all columns like INSERT; else we transmit
	 * only columns that were explicitly targets of the UPDATE, so as to avoid
	 * unnecessary data transmission.  (We can't do that for INSERT since we
	 * would miss sending default values for columns not listed in the source
	 * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
	 * those triggers might change values for non-target columns, in which
	 * case we would miss sending changed values for those columns.)
	 */
	if (operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row)
		 trigger_update = true;

	if (operation == CMD_INSERT || trigger_update)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
			char *attrname = NameStr(attr->attname);

			/*
			 * DynamoDB does not allow to update key columns. Therefore, skip key column when
			 * updating.
			 */
			if (trigger_update &&
				((!IS_KEY_EMPTY(partition_key) && strcmp(partition_key, attrname) == 0) ||
				 (!IS_KEY_EMPTY(sort_key) && strcmp(sort_key, attrname) == 0)))
				continue;

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		int			col = -1;
		Bitmapset  *allUpdatedCols = bms_union(rte->updatedCols, rte->extraUpdatedCols);
		ListCell   *lc;

		/*
		 * DynamoDB only support updating column
		 */
		foreach(lc, root->parse->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Node		*node = (Node *) tle->expr;

			if (nodeTag(node) == T_SubscriptingRef)
				elog(ERROR, "dynamodb_fdw: unsupported updating target");
		}

		while ((col = bms_next_member(allUpdatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "dynamodb_fdw: system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * Raise error if there is WITH CHECK OPTION
	 */
	if (plan->withCheckOptionLists)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("dynamodb_fdw: unsupported feature WITH CHECK OPTION")));
	}
		
	/*
	 *  Raise error if there is RETURNING
	 */
	if (plan->returningLists && operation == CMD_INSERT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("dynamodb_fdw: unsupported RETURNING in INSERT operation")));
	}

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * Raise error if there ON CONFLICT
	 */
	if (plan->onConflictAction != ONCONFLICT_NONE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("dynamodb_fdw: unsupported feature ON CONFLICT")));
	}

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			dynamodb_deparse_insert(&sql, rte, resultRelation, rel, targetAttrs,
									&retrieved_attrs);
			break;
		case CMD_UPDATE:
		{
			condAttr = dynamodb_get_key_names(tupdesc, foreignTableId, partition_key, sort_key);
			dynamodb_deparse_update(&sql, rte, resultRelation, rel, targetAttrs,
									withCheckOptionList, returningList, &retrieved_attrs, condAttr);
			break;
		}
		case CMD_DELETE:
		{
			condAttr = dynamodb_get_key_names(tupdesc, foreignTableId, partition_key, sort_key);
			dynamodb_deparse_delete(&sql, rte, resultRelation, rel, returningList,
									&retrieved_attrs, condAttr);
			break;
		}
		default:
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("dynamodb_fdw: unexpected operation")));
			break;
	}

	table_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make4(makeString(sql.data),
					  targetAttrs,
#if PG_VERSION_NUM >= 150000
					  makeBoolean((retrieved_attrs != NIL)),
#else
					  makeInteger((retrieved_attrs != NIL)),
#endif
					  retrieved_attrs);	
}

/*
 * dynamodbExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
extern "C" void
dynamodbExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *plan = castNode(ForeignScan, node->ss.ps.plan);
	List	   *fdw_private = plan->fdw_private;

	/*
	 * Add remote query, when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		char	   *sql;

		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}

/*
 * dynamodbExplainForeignModify
 *		Produce extra output for EXPLAIN of a ModifyTable on a foreign table
 */
extern "C" void
dynamodbExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo,
							 List *fdw_private,
							 int subplan_index,
							 ExplainState *es)
{
	if (es->verbose)
	{
		char	   *sql = strVal(list_nth(fdw_private, 0));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}

/*
 * dynamodbBeginForeignModify
 *
 * Begin an insert/update/delete operation on a foreign table
 */
extern "C" void
dynamodbBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
	DynamoDBFdwModifyState *fmstate;
	char	   *query;
	List	   *target_attrs;
	bool		has_returning;
	List	   *retrieved_attrs;
	RangeTblEntry *rte;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Deconstruct fdw_private data. */
	query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
	target_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
#if PG_VERSION_NUM >= 150000
	has_returning = boolVal(list_nth(fdw_private, FdwModifyPrivateHasReturning));
#else
	has_returning = intVal(list_nth(fdw_private, FdwModifyPrivateHasReturning));
#endif
	retrieved_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateRetrievedAttrs);

	/* Find RTE. */
	rte = exec_rt_fetch(resultRelInfo->ri_RangeTableIndex,
						mtstate->ps.state);

	/* Construct an execution state. */
	fmstate = dynamodb_create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									mtstate->operation,
#if (PG_VERSION_NUM >= 140000)
									outerPlanState(mtstate)->plan,
#else
									mtstate->mt_plans[subplan_index]->plan,
#endif
									query,
									target_attrs,
									has_returning,
									retrieved_attrs);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ForeignScanState *node)
{
	DynamoDBFdwScanState *fsstate = (DynamoDBFdwScanState *) node->fdw_state;

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->cursor_exists = true;
	fsstate->tuples = NULL;
	fsstate->eof_reached = false;
	fsstate->first_fetch = true;
	fsstate->next_token = NULL;
	fsstate->next_fetch_ready = true;
	fsstate->row_index = 0;
	fsstate->num_rows = 0;
}

/*
 * Fetch some more rows from the node's cursor.
 */
extern void
fetch_more_data(ForeignScanState *node)
{
	DynamoDBFdwScanState *fsstate = (DynamoDBFdwScanState *) node->fdw_state;
	MemoryContext oldcontext;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	PG_TRY();
	{
		DynamoDBClient  *conn = fsstate->conn;
		bool	has_more_rows = true;

		/*
		 * Only send request in 2 cases:
		 * 1. The first time fetching data: next_fetch_ready and first_fetch are true
		 * 2. Not the first time fetching data and there is more data to fetch in DynamoDB:
		 * next_fetch_ready is true, first_fetch is false and next_token is not empty
		 *
		 * If not the first time and next_token is empty, it means there is no more data
		 * in DynamoDB to fetch, the FDW stop fetching.
		 */
		if (fsstate->next_fetch_ready)
		{
			if (fsstate->first_fetch)
				fsstate->first_fetch = false;
			else if (!fsstate->next_token)
				has_more_rows = false;

			if (has_more_rows)
			{
				Model::ExecuteStatementRequest req;
				Aws::DynamoDB::Model::ExecuteStatementOutcome outcome;
				std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result;

				req.SetStatement(fsstate->query);

				/*
				 * Set next token to fetch the remaining data in DynamoDB.
				 * Only set next token if it is not empty.
				 */
				if (fsstate->next_token != NULL)
					req.SetNextToken(fsstate->next_token);

				outcome = conn->ExecuteStatement(req);
				if (!outcome.IsSuccess())
					dynamodb_report_error(ERROR, outcome.GetError().GetMessage(), fsstate->query);

				result = Aws::MakeShared<Aws::DynamoDB::Model::ExecuteStatementResult>(DYNAMODB_ALLOCATION_TAG, outcome.GetResult());

				if (result->GetItems().size() == 0)
					has_more_rows = false;

				/* Save next_token value for next fetch or reset if next_token is empty */
				if (result->GetNextToken().empty())
					fsstate->next_token = NULL;
				else
					fsstate->next_token = (char *) result->GetNextToken().c_str();

				fsstate->next_fetch_ready = false;
				fsstate->row_index = 0;
				fsstate->num_rows = result->GetItems().size();
				fsstate->result = std::move(result);
			}
		}

		/*
		 * Get one row per iterate.
		 */
		if (has_more_rows)
		{
			Assert(IsA(node->ss.ps.plan, ForeignScan));

			fsstate->tuples = make_tuple_from_result_row (fsstate->result, &fsstate->row_index,
														fsstate->rel, fsstate->retrieved_attrs,
														node, fsstate->temp_cxt);
		}

		/* Ready for next fetch if all rows has been processed */
		if (fsstate->row_index == fsstate->num_rows)
		{
			fsstate->next_fetch_ready = true;

			/* Must be EOF when there is no more data to fetch */
			if (fsstate->next_token == NULL)
				fsstate->eof_reached = true;
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);

}

static HeapTuple
make_tuple_from_result_row(std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result,
							unsigned int *row_index,
							Relation rel,
							List *retrieved_attrs,
							ForeignScanState *fsstate,
							MemoryContext temp_context)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	MemoryContext oldcontext;
	ListCell   *lc;
	const Aws::Vector<Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>>& items = result->GetItems();

	Assert(*row_index < items.size());

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(temp_context);

	/*
	 * Get the tuple descriptor for the row.  Use the rel's tupdesc if rel is
	 * provided, otherwise look to the scan node's ScanTupleSlot.
	 */
	if (fsstate)
		tupdesc = fsstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	else
		tupdesc = RelationGetDescr(rel);

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	/* Get the row based on row index */
	auto row = items.at(*row_index);

	lc = list_head(retrieved_attrs);

	while (lc != NULL)
	{
		Oid			pgtype;
		int32		pgtypmod;
		const char *attname;
		int			attnum;

		/*
		 * Attribute information are retrieved by a pair: first is attribute name,
		 * next is attribute number.
		 */
		attname = strVal(lfirst(lc));
		lc = lnext(retrieved_attrs, lc);
		attnum = intVal(lfirst(lc)) - 1;

		/*
		 * Skip columns which are not returned from DynamoDB.
		 * Those columns are null by default.
		 */
		for (const auto& column : row)
		{
			if (strcmp(attname, column.first.c_str()) != 0)
				continue;

			pgtype = TupleDescAttr(tupdesc, attnum)->atttypid;
			pgtypmod = TupleDescAttr(tupdesc, attnum)->atttypmod;

			if (column.second.GetType() != Aws::DynamoDB::Model::ValueType::NULLVALUE)
			{
				nulls[attnum] = false;
				values[attnum] = dynamodb_convert_to_pg(pgtype,
														pgtypmod,
														column.second);
			}
		}
		lc = lnext(retrieved_attrs, lc);
	}

	/* Increase row index to prepare for next fetch */
	(*row_index)++;

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	/* Clean up */
	MemoryContextReset(temp_context);

	return tuple;
}

/*
 * dynamodb_create_foreign_modify
 *
 * Construct an execution state of a foreign insert/update/delete
 * operation
 */
extern DynamoDBFdwModifyState *
dynamodb_create_foreign_modify(EState *estate,
					  RangeTblEntry *rte,
					  ResultRelInfo *resultRelInfo,
					  CmdType operation,
					  Plan *subplan,
					  char *query,
					  List *target_attrs,
					  bool has_returning,
					  List *retrieved_attrs)
{
	DynamoDBFdwModifyState *fmstate;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	Oid			userid;
	ForeignTable *table;
	UserMapping *user;
	Oid			foreignTableId = RelationGetRelid(rel);
	int			i;

	/* Begin constructing DynamoDBFdwModifyState. */
	fmstate = (DynamoDBFdwModifyState *) palloc0(sizeof(DynamoDBFdwModifyState));
	fmstate->rel = rel;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	user = GetUserMapping(userid, table->serverid);

	/* Open connection; report that we'll create a prepared statement. */
	fmstate->conn = dynamodb_get_connection(user);
	fmstate->p_name = NULL;		/* prepared statement not made yet */

	/* Set up remote query information. */
	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->retrieved_attrs = retrieved_attrs;

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "dynamodb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Initialize auxiliary state */
	fmstate->aux_fmstate = NULL;

	fmstate->junk_idx = (AttrNumber *) palloc0(RelationGetDescr(rel)->natts * sizeof(AttrNumber));
	/* loop through table columns */
	for (i = 0; i < RelationGetDescr(rel)->natts; ++i)
	{
		/*
		 * for partition key and sort key columns, get the resjunk attribute number and store
		 * it
		 */
		fmstate->junk_idx[i] =
			ExecFindJunkAttributeInTlist(subplan->targetlist,
										 get_attname(foreignTableId, i + 1, false));
	}

	return fmstate;
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
dynamodb_set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	/*
	 * In addition force restrictive search_path, in case there are any
	 * regproc or similar constants to be printed.
	 */
	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
dynamodb_reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}


Aws::DynamoDB::Model::ExecuteStatementOutcome
dynamodbOutcome(Aws::DynamoDB::DynamoDBClient *conn, 
	Aws::DynamoDB::Model::ExecuteStatementRequest req)
{
	Aws::DynamoDB::Model::ExecuteStatementOutcome outcome;
	outcome = conn->ExecuteStatement(req);
	return outcome;
}

/*
 * dynamodb_execute_foreign_modify
 *
 * Perform foreign-table modification as required, and fetch RETURNING
 * result if any.  (This is the shared guts of dynamodbExecForeignInsert,
 * dynamodbExecForeignUpdate, and dynamodbExecForeignDelete.)
 */
extern "C" TupleTableSlot *
dynamodb_execute_foreign_modify(EState *estate,
 					   ResultRelInfo *resultRelInfo,
 					   CmdType operation,
 					   TupleTableSlot *slot,
 					   TupleTableSlot *planSlot)
{
 	DynamoDBFdwModifyState *fmstate = (DynamoDBFdwModifyState *) resultRelInfo->ri_FdwState;
	ListCell   *lc;
	Datum		value = 0;
	int			bindnum = 0;
	Relation    rel = resultRelInfo->ri_RelationDesc;
    Oid         foreignTableId = RelationGetRelid(rel);
	dynamodb_opt *opt = dynamodb_get_options(foreignTableId);
	char *partition_key = opt->svr_partition_key;
	char *sort_key = opt -> svr_sort_key;
	Aws::DynamoDB::Model::ExecuteStatementRequest req;
	Aws::Vector<Aws::DynamoDB::Model::AttributeValue> values;
	Aws::DynamoDB::Model::AttributeValue bindval;
	Aws::DynamoDB::Model::ExecuteStatementOutcome outcome;

	Form_pg_attribute att;
	Oid			type;

	/* Binding values */
	foreach(lc, fmstate->target_attrs)
	{
		int		attnum = lfirst_int(lc) - 1;
		Oid		type = TupleDescAttr(slot->tts_tupleDescriptor, attnum)->atttypid;
		bool	isnull;

		value = slot_getattr(slot, attnum + 1, &isnull);
		bindval = dynamodb_bind_sql_var(type, bindnum, value, fmstate->query, isnull);
		values.push_back(bindval);
		bindnum++;
	}

	/* Bind where condition using junk column */
 	if (operation == CMD_UPDATE || operation == CMD_DELETE)
 	{
		if (IS_KEY_EMPTY(partition_key))
			elog(ERROR, "dynamodb_fdw: The partition_key option has not been set");
		for (int i = 0; i < slot->tts_tupleDescriptor->natts ; i++)
		{
			att = TupleDescAttr(slot->tts_tupleDescriptor, i);
			if (strcmp(opt->svr_partition_key, NameStr(att->attname)) == 0 ||
				IS_KEY_COLUMN(NameStr(att->attname), sort_key))
			{
				bool 		isnull;

				/* Get the id that was passed up as a resjunk column */
				value = ExecGetJunkAttribute(planSlot, fmstate->junk_idx[i], &isnull);
				type = att->atttypid;

				bindval = dynamodb_bind_sql_var(type, bindnum, value, fmstate->query, isnull);
				values.push_back(bindval);
				bindnum++;
			}
		}
	}

	/* Execute the query */
	req.SetStatement(fmstate->query);
	req.SetParameters(values);
	outcome = dynamodbOutcome(fmstate->conn, req);
	if (!outcome.IsSuccess())
		dynamodb_report_error(ERROR, outcome.GetError().GetMessage(), fmstate->query);

	/* Check number of rows affected, and fetch RETURNING tuple if any */
	if (fmstate->has_returning)
	{
		std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result(new Aws::DynamoDB::Model::ExecuteStatementResult());

		*result = outcome.GetResult();
		dynamodb_store_returning_result(fmstate, slot, result);
	}

	MemoryContextReset(fmstate->temp_cxt);
	return slot;
}

/*
 * store_returning_result
 *		Store the result of a RETURNING clause
 *
 * On error, be sure to release the PGresult on the way out.  Callers do not
 * have PG_TRY blocks to ensure this happens.
 */
static void
dynamodb_store_returning_result(DynamoDBFdwModifyState *fmstate,
								TupleTableSlot *slot,
								std::shared_ptr<Aws::DynamoDB::Model::ExecuteStatementResult> result)
{
	HeapTuple	newtup;
	unsigned int index = 0;

	newtup = make_tuple_from_result_row(result, &index,
										fmstate->rel,
										fmstate->retrieved_attrs,
										NULL,
										fmstate->temp_cxt);

	/*
	 * The returning slot will not necessarily be suitable to store
	 * heaptuples directly, so allow for conversion.
	 */
	ExecForceStoreHeapTuple(newtup, slot, true);
}

/*
 * dynamodbExecForeignInsert
 *
 * Insert one row into a foreign table
 */
extern "C" TupleTableSlot *
dynamodbExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	DynamoDBFdwModifyState *fmstate = (DynamoDBFdwModifyState *) resultRelInfo->ri_FdwState;
	TupleTableSlot *rslot;

	/*
	 * If the fmstate has aux_fmstate set, use the aux_fmstate (see
	 * dynamodbBeginForeignInsert())
	 */
	if (fmstate->aux_fmstate)
		resultRelInfo->ri_FdwState = fmstate->aux_fmstate;
	rslot = dynamodb_execute_foreign_modify(estate, resultRelInfo, CMD_INSERT, slot, planSlot);
	/* Revert that change */
	if (fmstate->aux_fmstate)
		resultRelInfo->ri_FdwState = fmstate;

	return rslot;
}

extern "C" TupleTableSlot *
dynamodbExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	return dynamodb_execute_foreign_modify(estate, resultRelInfo, CMD_UPDATE, slot, planSlot);
}

/*
 * dynamodbExecForeignDelete
 *		Delete one row from a foreign table
 */
extern "C" TupleTableSlot *
dynamodbExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	return dynamodb_execute_foreign_modify(estate, resultRelInfo, CMD_DELETE,
								  		   slot, planSlot);
}
/*
 * dynamodbEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
extern "C" void
dynamodbEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	DynamoDBFdwModifyState *fmstate = (DynamoDBFdwModifyState *) resultRelInfo->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	/* Destroy the execution state */
	if (fmstate && fmstate->query)
	{
		fmstate->query = NULL;
	}
}

extern "C" void
dynamodbBeginForeignInsert(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
            (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
             errmsg("COPY and foreign partition routing not supported in dynamodb_fdw")));
}

extern "C" void
dynamodbEndForeignInsert(EState *estate,
						ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
            (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
             errmsg("COPY and foreign partition routing not supported in dynamodb_fdw")));
}

/*
 * dynamodb_get_key_names
 * 		Add all primary key attribute names to condAttr used in where clause of update
 */
static List *
dynamodb_get_key_names(TupleDesc tupdesc, Oid foreignTableId, char *partition_key, char *sort_key)
{
	List *condAttr = NIL;
	int			i;

	for (i = 0; i < tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber	attrno = att->attnum;
		char	   *colname = get_attname(foreignTableId, attrno, false);

		if (IS_KEY_COLUMN(colname, partition_key) || IS_KEY_COLUMN(colname, sort_key))
			condAttr = lappend_int(condAttr, attrno);
	}

	return condAttr;
}
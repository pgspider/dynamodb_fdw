/*-------------------------------------------------------------------------
 *
 * dynamodb_fdw.c
 *		  FDW routines for dynamodb_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/dynamodb_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <limits.h>

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
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"
#include "dynamodb_fdw.h"
#include "storage/ipc.h"

PG_MODULE_MAGIC;

void _PG_init(void);
extern void dynamodb_init();
extern void dynamodb_shutdown();

/*
 * FDW callback routines
 */
extern void dynamodbGetForeignRelSize(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
extern void dynamodbGetForeignPaths(PlannerInfo *root,
									RelOptInfo *baserel,
									Oid foreigntableid);
extern ForeignScan *dynamodbGetForeignPlan(PlannerInfo *root,
										   RelOptInfo *foreignrel,
										   Oid foreigntableid,
										   ForeignPath *best_path,
										   List *tlist,
										   List *scan_clauses,
										   Plan *outer_plan);
extern void dynamodbBeginForeignScan(ForeignScanState *node, int eflags);
extern TupleTableSlot *dynamodbIterateForeignScan(ForeignScanState *node);
extern void dynamodbReScanForeignScan(ForeignScanState *node);
extern void dynamodbEndForeignScan(ForeignScanState *node);
extern void dynamodbAddForeignUpdateTargets(Query *parsetree,
											RangeTblEntry *target_rte,
											Relation target_relation);
extern List *dynamodbPlanForeignModify(PlannerInfo *root,
									   ModifyTable *plan,
									   Index resultRelation,
									   int subplan_index);
extern void dynamodbBeginForeignModify(ModifyTableState *mtstate,
									   ResultRelInfo *resultRelInfo,
									   List *fdw_private,
									   int subplan_index,
									   int eflags);
extern TupleTableSlot *execute_foreign_modify(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  CmdType operation,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
extern TupleTableSlot *dynamodbExecForeignInsert(EState *estate,
												 ResultRelInfo *resultRelInfo,
												 TupleTableSlot *slot,
												 TupleTableSlot *planSlot);
extern TupleTableSlot *dynamodbExecForeignUpdate(EState *estate,
												 ResultRelInfo *resultRelInfo,
												 TupleTableSlot *slot,
												 TupleTableSlot *planSlot);
extern TupleTableSlot *dynamodbExecForeignDelete(EState *estate,
												 ResultRelInfo *resultRelInfo,
												 TupleTableSlot *slot,
												 TupleTableSlot *planSlot);
extern void dynamodbEndForeignModify(EState *estate,
									 ResultRelInfo *resultRelInfo);

extern void dynamodbExplainForeignScan(ForeignScanState *node,
									   ExplainState *es);
extern void dynamodbExplainForeignModify(ModifyTableState *mtstate,
										ResultRelInfo *rinfo,
										List *fdw_private,
										int subplan_index,
										ExplainState *es);
void
_PG_init(void)
{
    dynamodb_init();  
    on_proc_exit(&dynamodb_shutdown, PointerGetDatum(NULL));
}


PG_FUNCTION_INFO_V1(dynamodb_fdw_version);

Datum
dynamodb_fdw_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(CODE_VERSION);
}

PG_FUNCTION_INFO_V1(dynamodb_fdw_handler);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
dynamodb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = dynamodbGetForeignRelSize;
	routine->GetForeignPaths = dynamodbGetForeignPaths;
	routine->GetForeignPlan = dynamodbGetForeignPlan;
	routine->BeginForeignScan = dynamodbBeginForeignScan;
	routine->IterateForeignScan = dynamodbIterateForeignScan;
	routine->ReScanForeignScan = dynamodbReScanForeignScan;
	routine->EndForeignScan = dynamodbEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = dynamodbAddForeignUpdateTargets;
	routine->PlanForeignModify = dynamodbPlanForeignModify;
	routine->BeginForeignModify = dynamodbBeginForeignModify;
	routine->ExecForeignInsert = dynamodbExecForeignInsert;
	routine->ExecForeignUpdate = dynamodbExecForeignUpdate;
	routine->ExecForeignDelete = dynamodbExecForeignDelete;
	routine->EndForeignModify = dynamodbEndForeignModify;

	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = dynamodbExplainForeignScan;
	routine->ExplainForeignModify = dynamodbExplainForeignModify;

	PG_RETURN_POINTER(routine);
}

/*-------------------------------------------------------------------------
 *
 * deparse.cpp
 *		  Query deparser for dynamodb_fdw
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the remote server for execution, as
 * well as functions to construct the query text to be sent.  The latter
 * functionality is annoyingly duplicative of ruleutils.c, but there are
 * enough special considerations that it seems best to keep this separate.
 * One saving grace is that we only need deparse logic for node types that
 * we consider safe to send.
 *
 * We assume that the remote session's search_path is exactly "pg_catalog",
 * and thus we need schema-qualify all and only names outside pg_catalog.
 *
 * We do not consider that it is ever safe to send COLLATE expressions to
 * the remote server: it might not have the same collation names we do.
 * (Later we might consider it safe to send COLLATE "C", but even that would
 * fail on old remote servers.)  An expression is considered safe to send
 * only if all operator/function input collations used in it are traceable to
 * Var(s) of the foreign table.  That implies that if the remote server gets
 * a different answer than we do, the foreign table's columns are not marked
 * with collations that match the remote table's columns, which we can
 * consider to be user error.
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/dynamodb_fdw/deparse.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "dynamodb_fdw.hpp"

extern "C"
{
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "common/keywords.h"
#include "ctype.h"
#include "jansson.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/bitmapset.h"
#include "optimizer/optimizer.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "postgres.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
}

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type, or
								 * it has default collation that is not
								 * traceable to a foreign Var */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation is non-default and derives from
								 * something other than a foreign Var */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */
	StringInfo	buf;			/* output buffer to append to */
	bool		has_arrow;		/* True if expression contain arrow operators */
	List	  **attrs_list;		/* List of attributes */
} deparse_expr_cxt;

/*
 * Struct to pull out attribute name
 */
typedef struct pull_attribute_name_context
{
	StringInfo		attribute_name;	/* The target attribute name */
	bool			list_appended;	/* True if both attribute name
									   and list number has been
									   appended into attribute_name*/
	PlannerInfo	   *root;			/* The information for planning */
	int				list_num;		/* Number of nested list in the expression*/
} pull_attribute_name_context;

#define REL_ALIAS_PREFIX	"r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)	\
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX	"s"
#define SUBQUERY_COL_ALIAS_PREFIX	"c"

static const char *compOpName[] =
{
	/* Operator name */
	"<", 	/* Less than */
	">",	/* Greater than */
	"<=",	/* Less than or equal */
	">=",	/* Greater than or equal */
	"=",	/* Equal */
	"!=",	/* Not equal */
	"<>",	/* Not equal */
	NULL,	/* NULL */
};
static const char *jsonOpName[] =
{
	/* Operator name */
	"->",	/* json arrow operator */
	"->>",	/* json arrow operator */
	NULL,	/* NULL */
};

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool dynamodb_foreign_expr_walker(Node *node,
								foreign_glob_cxt *glob_cxt,
								foreign_loc_cxt *outer_cxt);

/*
 * Functions to construct string representation of a node tree.
 */
static void dynamodb_deparse_target_list(StringInfo buf,
										RangeTblEntry *rte,
										Index rtindex,
										Relation rel,
										bool is_returning,
										Bitmapset *attrs_used,
										bool qualify_col,
										List **retrieved_attrs);
static void dynamodb_deparse_column_ref(StringInfo buf, int varno,
										int varattno, RangeTblEntry *rte,
										List **retrieved_attrs,
										bool need_store_attr);
static void dynamodb_deparse_relation(StringInfo buf, Relation rel);
static void dynamodb_deparse_expr(Expr *expr, deparse_expr_cxt *context);
static void dynamodb_deparse_var(Var *node, deparse_expr_cxt *context);
static void dynamodb_deparse_const(Const *node, deparse_expr_cxt *context);
static void dynamodb_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void dynamodb_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void dynamodb_deparse_operator_name(StringInfo buf, Form_pg_operator opform);
static void dynamodb_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
												deparse_expr_cxt *context);
static void dynamodb_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context);
static void dynamodb_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void dynamodb_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void dynamodb_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
											RelOptInfo *foreignrel);
static void dynamodb_deparse_returning_list(StringInfo buf, RangeTblEntry *rte,
											Index rtindex, Relation rel,
											bool trig_after_row,
											List *withCheckOptionList,
											List *returningList,
											List **retrieved_attrs,
											bool is_delete);
static void dynamodb_deparse_from_expr(List *quals, deparse_expr_cxt *context);
static void dynamodb_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context);
static void dynamodb_append_conditions(List *exprs, deparse_expr_cxt *context);
Form_pg_operator dynamodb_get_operator_expression(Oid oid);
static char *dynamodb_replace_operator(char *in);
DynamoDBOperatorsSupport dynamodb_validate_operator_name(Form_pg_operator opform);
static void dynamodb_store_attr_info(const char *col_name, int varno, List **retrieved_attr);
static void dynamodb_pull_attribute_name_walker(Node *node, pull_attribute_name_context *context);
static char *dynamodb_get_attribute_name(Node *node, PlannerInfo *root);
static char *dynamodb_get_column_name(Oid relid, int varattno);
void dynamodb_get_document_path(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, Expr *expr);

/*
 * dynamodb_classify_conditions
 *
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
dynamodb_classify_conditions(PlannerInfo *root,
				   RelOptInfo *baserel,
				   List *input_conds,
				   List **remote_conds,
				   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);

		/*
		 * DynamoDB does not support condition with a boolean column only
		 * Example: WHERE c1;
		 */
		if (nodeTag(ri->clause) == T_Var)
		{
			*local_conds = lappend(*local_conds, ri);
			continue;
		}

		if (dynamodb_is_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/*
 * dynamodb_quote_identifier
 *
 * Quote an identifier only if needed.
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
const char *
dynamodb_quote_identifier(const char *ident)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter, a uppercase letter or underscore
	 * and contains only lowercase letters, uppercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int			nquotes = 0;
	bool		safe;
	const char *ptr;
	char	   *result;
	char	   *optr;

	/*
	 * would like to use <ctype.h> macros here, but they might yield unwanted
	 * locale-specific results...
	 */
	safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_' || (ident[0] >= 'A' && ident[0] <= 'Z'));

	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			(ch == '_'))
		{
			/* okay */
		}
		else
		{
			safe = false;
			if (ch == '"')
				nquotes++;
		}
	}

	if (safe)
		return ident;			/* no change needed */

	result = (char *) palloc(strlen(ident) + nquotes + 2 + 1);

	optr = result;
	*optr++ = '"';
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if (ch == '"')
			*optr++ = '"';
		*optr++ = ch;
	}
	*optr++ = '"';
	*optr = '\0';

	return result;
}

/*
 * dynamodb_is_foreign_expr
 *
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
dynamodb_is_foreign_expr(PlannerInfo *root,
				RelOptInfo *baserel,
				Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	/*
	 * For base relation, use its own relids.
	 */
	glob_cxt.relids = baserel->relids;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!dynamodb_foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * dynamodb_foreign_expr_walker
 *
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool
dynamodb_foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	DynamoDBFdwRelationInfo *fpinfo;
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* May need server info from baserel's fdw_private struct */
	fpinfo = (DynamoDBFdwRelationInfo *) (glob_cxt->foreignrel->fdw_private);

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, ie it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */

					/*
					 * System columns other than ctid should not be sent to
					 * the remote, since we don't make any effort to ensure
					 * that local and remote values match (tableoid, in
					 * particular, almost certainly doesn't match).
					 */
					if (var->varattno < 0 &&
						var->varattno != SelfItemPointerAttributeNumber)
						return false;

					/* Else check the collation */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Parameter is unsupported */
					return false;
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr.
				 * It's unsafe to send to the remote unless it's used in a
				 * non-collation-sensitive context.
				 */
				collation = c->constcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr	*fe = (FuncExpr *) node;
				char		*opername = NULL;

				/* get function name */
				opername = get_func_name(fe->funcid);

				/* check NULL for opername */
				if (opername == NULL)
					elog(ERROR, "dynamodb_fdw: cache lookup failed for function %u", fe->funcid);

				if (strcmp(opername, "size") == 0)
				{
					Expr *arg = (Expr *) linitial(fe->args);

					/* Do not push down if user does not input Var as argument */
					if (nodeTag(arg) != T_Var)
						return false;
				}
				else
					return false;
				/*
				 * Recurse to input subexpressions.
				 */
				if (!dynamodb_foreign_expr_walker((Node *) fe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (fe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 fe->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = fe->funccollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	   *oe = (OpExpr *) node;
				Form_pg_operator form;
				DynamoDBOperatorsSupport	opkind;
				char	   *opname;

				/*
				 * Similarly, only shippable operators can be sent to remote.
				 * (If the operator is shippable, we assume its underlying
				 * function is too.)
				 */
				if (!dynamodb_is_shippable(oe->opno, OperatorRelationId, fpinfo))
					return false;

				form = dynamodb_get_operator_expression(oe->opno);
				opname = form->oprname.data;

				/*
				 * DynamoDB only support condition with the following syntax:
				 * Operand comparison_operator Operand
				 */
				if (!(form->oprkind == 'b' && list_length(oe->args) == 2))
					return false;

				opkind = dynamodb_validate_operator_name(form);

				/* Return false if operator name is not supported */
				if (opkind == OP_UNSUPPORT)
					return false;
				else if (opkind == OP_JSON)
				{
					/*
					 * The right operand must be a constant
					 */
					Expr *expr = (Expr *)lfirst(list_tail(oe->args));

					if (!IsA(expr, Const))
						return false;
					else
					{
						/* Do not push down if the right operand is a negative number */
						Const   *c = (Const *) expr;

						if (c->consttype == INT2OID ||
							c->consttype == INT4OID ||
							c->consttype == INT8OID)
						{
							int32	dat = DatumGetInt32(c->constvalue);

							if (dat < 0)
								return false;
						}
					}
				}
				else if (opkind == OP_CONDITIONAL)
				{
					Expr   *left = (Expr *)lfirst(list_head(oe->args));
					Expr   *right = (Expr *)lfirst(list_tail(oe->args));
					Expr   *expr = NULL;
					Const  *c;
					bool	has_const = false;

					if (nodeTag(left) == T_Const)
					{
						expr = left;
						has_const = true;
					}
					else if (nodeTag(right) == T_Const)
					{
						expr = right;
						has_const = true;
					}

					if (has_const)
					{
						c = (Const *) expr;

						/* Do not push down when comparing with array */
						if (c->consttype == INT2ARRAYOID ||
							c->consttype == INT4ARRAYOID ||
							c->consttype == INT8ARRAYOID ||
							c->consttype == FLOAT4ARRAYOID ||
							c->consttype == FLOAT8ARRAYOID ||
							c->consttype == NUMERICARRAYOID ||
							c->consttype == VARCHARARRAYOID ||
							c->consttype == TEXTARRAYOID ||
							c->consttype == BPCHARARRAYOID ||
							c->consttype == NAMEARRAYOID)
							return false;

						/* Do not push down when comparing text using <, >, <=, >= */
						if ((c->consttype == TEXTOID ||
							c->consttype == VARCHAROID ||
							c->consttype == BPCHAROID ||
							c->consttype == NAMEOID ||
							c->consttype == JSONBOID ||
							c->consttype == JSONOID) &&
							((strcmp(opname, "<") == 0 ||
							strcmp(opname, "<=") == 0 ||
							strcmp(opname, ">") == 0 ||
							strcmp(opname, ">=") == 0)))
							return false;
					}
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!dynamodb_foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;


				if (opkind != OP_JSON)
				{
					/*
					* If operator's input collation is not derived from a foreign
					* Var, it can't be sent to remote.
					*/
					if (oe->inputcollid == InvalidOid)
						/* OK, inputs are all noncollatable */ ;
					else if (inner_cxt.state != FDW_COLLATE_SAFE ||
							oe->inputcollid != inner_cxt.collation)
						return false;
				}

				/* Result-collation handling is same as for functions */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;
				Form_pg_operator	form;
				char	   *opname = NULL;
				Expr	   *arg1;
				Expr	   *arg2;

				/*
				 * Again, only shippable operators can be sent to remote.
				 */
				if (!dynamodb_is_shippable(oe->opno, OperatorRelationId, fpinfo))
					return false;

				form = dynamodb_get_operator_expression(oe->opno);
				opname = form->oprname.data;

				/* Only support push down equal or not-equal operator. */
				if (!(strcmp(opname, "=") == 0 ||
					  strcmp(opname, "<>") == 0 ||
					  strcmp(opname, "!=") == 0))
					return false;

				arg1 = (Expr *) linitial(oe->args);
				arg2 = (Expr *) lsecond(oe->args);

				/*
				 * Do not push down when the first argument exist
				 * in the array because DynamoDB does not support it
				 * Example: c1 = ANY(ARRAY(c1, c2))
				 */
				if (nodeTag(arg2) == T_ArrayExpr)
				{
					ArrayExpr  *a = (ArrayExpr *) arg2;

					if (list_member(a->elements, arg1))
						return false;
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!dynamodb_foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				List	   *l = (List *) b->args;
				ListCell   *lc;

				/*
				 * DynamoDB does not support the case only column as operand.
				 * Example: WHERE NOT c1; WHERE c1 OR condition
				 */
				foreach(lc, l)
				{
					if (nodeTag(lfirst(lc)) == T_Var)
						return false;
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!dynamodb_foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!dynamodb_foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!dynamodb_foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!dynamodb_foreign_expr_walker((Node *) a->elements,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * ArrayExpr must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a function).
				 */
				collation = a->array_collid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		default:
			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not shippable, it can't be sent
	 * to remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !dynamodb_is_shippable(exprType(node), TypeRelationId, fpinfo))
		return false;

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * dynamodb_deparse_select_stmt_for_rel
 *
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void
dynamodb_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
									List *tlist, List *remote_conds, List *pathkeys,
									List **retrieved_attrs)
{
    deparse_expr_cxt context;
	List	   *quals;

	/* Fill portions of context common to upper, join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.scanrel = rel;
	context.has_arrow = false;
	context.attrs_list = retrieved_attrs;

	/* Construct SELECT clause */
	dynamodb_deparse_select(tlist, retrieved_attrs, &context);

	/*
	 * We can use the supplied list of remote conditions directly to build the WHERE clause.
	 */
	quals = remote_conds;

	/* Construct FROM and WHERE clauses */
	dynamodb_deparse_from_expr(quals, &context);
}

/*
 * dynamodb_pull_attribute_name_walker
 *
 * Recursively go through the operator expression to get the attribute name.
 * DynamoDB returns the attribute name in result based on the following rule:
 * 1. If user selects an attribute directly, it returns the attribute name directly.
 *    Example:	(SQL): SELECT c1
 * 				(PartiQL): SELECT c1
 * 	  => attribute name has name 'c1'.
 * 2. If user selects objects which is nested inside a List, DynamoDB returns
 *    a combination of the attribute name right before list number and the list
 *    number (inside []).
 *    Example (SQL): SELECT c1->c2->c3->3
 * 			  (PartiQL): SELECT c1.c2.c3[3]
 * 	  => attribute name has name 'c3[3]'.
 * 3. If user selects objects which is nested inside a Map, DynamoDB returns
 *	  the attribute name of the last nested object.
 *	  Example (SQL): SELECT c1->c2->c3
 *			  (PartiQL): SELECT c1.c2.c3
 *	  => attribute name has name 'c3'
 * 4. If user selects objects which is deeply nested inside List and Maps, DynamoDB
 * 	  returns a combination of the attribute name right before the left most list number
 * 	  and the list number (inside []).
 *	  Example (SQL): SELECT c1->c2->c3->3->c4->5->c6->c7.
 *			  (PartiQL): SELECT c1.c2.c3[3].c4[5].c6.c7
 *	  => attribute name has name 'c3[3]'.
 */
static void
dynamodb_pull_attribute_name_walker(Node *node, pull_attribute_name_context *context)
{
	if (node == NULL)
		return;

	/*
	 * DynamoDB FDW only supports selecting columns or JSON nested objects,
	 * so no need to handle other kinds of nodes
	 */
	switch (nodeTag(node))
	{
		case T_OpExpr:
			{
				OpExpr	   *oe = (OpExpr *) node;
				Form_pg_operator form;
				DynamoDBOperatorsSupport	opkind;
				ListCell   *arg;

				form = dynamodb_get_operator_expression(oe->opno);
				opkind = dynamodb_validate_operator_name(form);

				/*
				 * For JSON operator expression, the node tree is as following:
				 *								Operator
				 *						Operator		right-operand
				 *				Operator	right-operand
				 *		Operator	right-operand
				 * The checking process will start from the right to left.
				 */
				if (opkind == OP_JSON)
				{
					Expr	   *rightop;
					Oid			typoutput;
					bool		typIsVarlena;
					char	   *extval;
					Oid			consttype;

					rightop = (Expr *) lfirst(list_tail(oe->args));
					consttype = ((Const *) rightop)->consttype;

					getTypeOutputInfo(consttype,
							&typoutput, &typIsVarlena);
					extval = OidOutputFunctionCall(typoutput, ((Const *) rightop)->constvalue);

					if (consttype == INT2OID ||
						consttype == INT4OID ||
						consttype == INT8OID)
					{
						/*
						 * If detect any List, clear the old data of attribute name,
						 * save the list number in attribute name.
						 * Example: attribute name is [3]
						 */
						context->list_appended = true;

						/* Reset attribute name */
						resetStringInfo(context->attribute_name);
						appendStringInfo(context->attribute_name, "[%s]", extval);
					}
					else if (consttype == TEXTOID)
					{
						if (context->list_appended)
						{
							/*
							 * If the list number has been saved before,
							 * prepend the attribute name into it.
							 * Example: attribute name is key3[3]
							 */
							char *tmp = pstrdup(context->attribute_name->data);

							resetStringInfo(context->attribute_name);
							appendStringInfo(context->attribute_name, "%s%s", extval, tmp);

							/* Reset list_appended to prepare for the case there are many nested list */
							context->list_appended = false;
							context->list_num++;
						}
						else if (strcmp(context->attribute_name->data, "") == 0)
						{
							/*
							 * If no list number and no attribute have been saved,
							 * it means current attribute is the right most attribute,
							 * only need to save it.
							 * Example: attribute name is key3[3]
							 */
							appendStringInfo(context->attribute_name, "%s", extval);
						}
					}
				}
				/* Recursive check the left operand */
				arg = list_head(oe->args);
				dynamodb_pull_attribute_name_walker((Node *) lfirst(arg), context);
				break;
			}
		case T_Var:
			{
				Var			   *v = (Var *) node;
				RangeTblEntry  *rte = planner_rt_fetch(v->varno, context->root);
				char		   *colname = NULL;

				colname = dynamodb_get_column_name(rte->relid, v->varattno);

				if (context->attribute_name->len > 0)
				{
					/*
					 * If there is nested list but the list_num is 0,
					 * it means only list number is added. The attribute name
					 * has not been added yet => prepend it.
					 */
					if (context->list_appended && context->list_num == 0)
					{
						char *tmp = pstrdup(context->attribute_name->data);

						resetStringInfo(context->attribute_name);
						appendStringInfo(context->attribute_name, "%s%s",
											colname, tmp);
					}
				}
				else
				{
					/*
					 * No nested object. Only need to save the attribute name.
					 */
					appendStringInfo(context->attribute_name, "%s", colname);
				}

				break;
			}
		default:
			break;
	}
}

/*
 * dynamodb_get_attribute_name
 *
 * Get the attribute name (which is equal to the attribute
 * name returned from DynamoDB) from a node.
 */
static char *
dynamodb_get_attribute_name(Node *node, PlannerInfo *root)
{
	pull_attribute_name_context context;
	context.attribute_name = makeStringInfo();
	context.root = root;
	context.list_appended = false;
	context.list_num = 0;

	dynamodb_pull_attribute_name_walker(node, &context);

	return context.attribute_name->data;
}

/*
 * dynamodb_deparse_select
 *
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... ".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns.  is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of deparseSelectStmtForRel() for details.
 */
static void
dynamodb_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	PlannerInfo *root = context->root;
	DynamoDBFdwRelationInfo *fpinfo = (DynamoDBFdwRelationInfo *) foreignrel->fdw_private;

	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");

    if (tlist != NULL)
	{
		ListCell *cell;
		int i = 0;
		bool first;

		first = true;
		*retrieved_attrs = NIL;

		foreach (cell, tlist)
		{
			Expr *expr = ((TargetEntry *)lfirst(cell))->expr;
			char *attr_name;

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			/* Deparse target list for push down */
			dynamodb_deparse_expr(expr, context);

			/* Store the attribute information */
			attr_name = dynamodb_get_attribute_name((Node *) expr, root);
			dynamodb_store_attr_info(attr_name, i + 1, context->attrs_list);
			i++;
		}
	}
	else
	{
		/*
		 * For a base relation fpinfo->attrs_used gives the list of columns
		 * required to be fetched from the foreign server.
		 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = table_open(rte->relid, NoLock);

		dynamodb_deparse_target_list(buf, rte, foreignrel->relid, rel, false,
						  fpinfo->attrs_used, false, retrieved_attrs);
		table_close(rel, NoLock);
	}
}

/*
 * dynamodb_deparse_from_expr
 *
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 * (These may or may not include RestrictInfo decoration.)
 */
static void
dynamodb_deparse_from_expr(List *quals, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;

	/* For upper relations, scanrel must be either a joinrel or a baserel */
	Assert(IS_SIMPLE_REL(scanrel));

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	dynamodb_deparse_from_expr_for_rel(buf, context->root, scanrel);

	/* Construct WHERE clause */
	if (quals != NIL)
	{
		appendStringInfoString(buf, " WHERE ");
		dynamodb_append_conditions(quals, context);
	}
}

/*
 * dynamodb_deparse_returning
 *
 * Deparse the RETURNING clause.
 * Store the necessarry attribute information.
 */
static void
dynamodb_deparse_returning(StringInfo buf,
							RangeTblEntry *rte,
							Index rtindex,
							Relation rel,
							bool is_delete,
							Bitmapset *attrs_used,
							List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;

	*retrieved_attrs = NIL;

	if (is_delete)
		appendStringInfoString(buf, " RETURNING ALL OLD *");
	else
		appendStringInfoString(buf, " RETURNING ALL NEW *");

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						attrs_used))
			dynamodb_store_attr_info(attr->attname.data, i, retrieved_attrs);
	}
}

/*
 * dynamodb_deparse_returning_list
 *
 * Add a RETURNING clause, if needed, to an UPDATE/DELETE.
 */
static void
dynamodb_deparse_returning_list(StringInfo buf, RangeTblEntry *rte,
					 Index rtindex, Relation rel,
					 bool trig_after_row,
					 List *withCheckOptionList,
					 List *returningList,
					 List **retrieved_attrs,
					 bool is_delete)
{
	Bitmapset  *attrs_used = NULL;

	if (returningList != NIL)
	{
		/*
		 * We need the attrs, non-system and system, mentioned in the local
		 * query's RETURNING list.
		 */
		pull_varattnos((Node *) returningList, rtindex,
					   &attrs_used);
	}

	if (attrs_used != NULL)
		dynamodb_deparse_returning(buf, rte, rtindex, rel, is_delete, attrs_used,
						  retrieved_attrs);
	else
		*retrieved_attrs = NIL;
}

/*
 * dynamodb_deparse_target_list
 *
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 *
 * If qualify_col is true, add relation alias before the column name.
 */
static void
dynamodb_deparse_target_list(StringInfo buf,
							RangeTblEntry *rte,
							Index rtindex,
							Relation rel,
							bool is_returning,
							Bitmapset *attrs_used,
							bool qualify_col,
							List **retrieved_attrs)
{
	TupleDesc   tupdesc = RelationGetDescr(rel);
	bool        have_wholerow;
	bool        first;
	int         i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								attrs_used);

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");

			first = false;

			dynamodb_deparse_column_ref(buf, rtindex, i, rte, retrieved_attrs, true);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first && !is_returning)
		appendStringInfoString(buf, "*");
}

/*
 * dynamodb_append_conditions
 *
 * Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to
 * deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos
 * or bare clauses.
 */
static void
dynamodb_append_conditions(List *exprs, deparse_expr_cxt *context)
{
	int			nestlevel;
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = dynamodb_set_transmission_modes();

	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *) expr)->clause;

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		dynamodb_deparse_expr((Expr *) expr, context);

		is_first = false;
	}

	dynamodb_reset_transmission_modes(nestlevel);
}

/*
 * dynamodb_deparse_from_expr_for_rel
 *
 * Construct FROM clause for given relation
 *
 * For a base relation it just returns schema-qualified tablename, 
 * with the appropriate alias if so requested.
 *
 * 'ignore_rel' is either zero or the RT index of a target relation.  In the
 * latter case the function constructs FROM clause of UPDATE or USING clause
 * of DELETE; it deparses the join relation as if the relation never contained
 * the target relation, and creates a List of conditions to be deparsed into
 * the top-level WHERE clause, which is returned to *ignore_conds.
 */
static void
dynamodb_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel)
{
    RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

    /*
	 * Core code already has some lock on each rel being planned, so we
	 * can use NoLock here.
	 */
    Relation	rel = table_open(rte->relid, NoLock);

    dynamodb_deparse_relation(buf, rel);

    table_close(rel, NoLock);
}

/*
 * dynamodb_deparse_column_ref
 *
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
dynamodb_deparse_column_ref(StringInfo buf, int varno, int varattno, RangeTblEntry *rte,
							List **retrieved_attrs, bool need_store_attr)
{
	char	   *colname = NULL;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	colname = dynamodb_get_column_name(rte->relid, varattno);

	appendStringInfoString(buf, dynamodb_quote_identifier(colname));

	if (need_store_attr)
		dynamodb_store_attr_info(colname, varattno, retrieved_attrs);
}

/*
 * dynamodb_deparse_relation
 *
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void
dynamodb_deparse_relation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *relname = NULL;
	ListCell   *lc;

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table_name") == 0)
			relname = defGetString(def);
	}

	/*
	 * Note: we could skip printing the schema name if it's pg_catalog, but
	 * that doesn't seem worth the trouble.
	 */
	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfo(buf, "%s",
					 dynamodb_quote_identifier(relname));
}

/*
 * dynamodb_deparse_string_literal
 *
 * Append a SQL string literal representing "val" to buf.
 */
void
dynamodb_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if ((ch) == '\'')
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * dynamodb_deparse_expr
 *
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
dynamodb_deparse_expr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			dynamodb_deparse_var((Var *) node, context);
			break;
		case T_Const:
			dynamodb_deparse_const((Const *) node, context);
			break;
		case T_FuncExpr:
			dynamodb_deparse_func_expr((FuncExpr *) node, context);
			break;
		case T_OpExpr:
			dynamodb_deparse_op_expr((OpExpr *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			dynamodb_deparse_scalar_array_op_expr((ScalarArrayOpExpr *) node, context);
			break;
		case T_BoolExpr:
			dynamodb_deparse_bool_expr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			dynamodb_deparse_null_test((NullTest *) node, context);
			break;
		case T_ArrayExpr:
			dynamodb_deparse_array_expr((ArrayExpr *) node, context);
			break;
		default:
			elog(ERROR, "dynamodb_fdw: unsupported expression type for deparse: %d", (int) nodeTag(node));
			break;
	}
}

/*
 * dynamodb_deparse_var
 *
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
dynamodb_deparse_var(Var *node, deparse_expr_cxt *context)
{
	Relids		relids = context->scanrel->relids;

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
		dynamodb_deparse_column_ref(context->buf, node->varno, node->varattno,
						 planner_rt_fetch(node->varno, context->root),
						 context->attrs_list, false);
	else
	{
		/* Does not reach here. */
		elog(ERROR, "dynamodb_fdw: Parameter is not supported");
	}
}

/*
 * dynamodb_deparse_json_value
 *
 * Deparse given json value into corresponding syntax of DynamoDB.
 */
static void
dynamodb_deparse_json_value(json_t *root, StringInfo buf)
{
	switch(root->type)
	{
		case JSON_STRING:
			{
				appendStringInfo(buf, "\'%s\'", json_string_value(root)?json_string_value(root):"");
				break;
			}
		case JSON_INTEGER:
			{
				appendStringInfo(buf, "%d", (int) json_integer_value(root));
				break;
			}
		case JSON_REAL:
			{
				appendStringInfo(buf, "%f", json_real_value(root));
				break;
			}
		case JSON_TRUE:
			{
				appendStringInfoString(buf, "TRUE");
				break;
			}
		case JSON_FALSE:
			{
				appendStringInfoString(buf, "FALSE");
				break;
			}
		case JSON_NULL:
			{
				appendStringInfoString(buf, "NULL");
				break;
			}
		case JSON_ARRAY:
			{
				json_t *value;
				bool	first = true;
				unsigned int	i;

				appendStringInfoString(buf, "<<");
				json_array_foreach(root, i, value)
				{
					if (!first)
						appendStringInfoChar(buf, ',');

					dynamodb_deparse_json_value(value, buf);
					first = false;
				}
				appendStringInfoString(buf, ">>");
				break;
			}
		case JSON_OBJECT:
			{
				bool	first = true;
				const char *key = NULL;
				json_t *element = NULL;

				appendStringInfoString(buf, "{");
				json_object_foreach(root, key, element)
				{
					if (!first)
						appendStringInfoChar(buf, ',');

					appendStringInfo(buf, "\'%s\': ", key);

					dynamodb_deparse_json_value(element, buf);
					first = false;
				}
				appendStringInfoString(buf, "}");
				break;
			}
	}
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
dynamodb_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoString(buf, "<<");
	foreach(lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		dynamodb_deparse_expr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoString(buf, ">>");
}

/*
 * dynamodb_deparse_const
 *
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 */
static void
dynamodb_deparse_const(Const *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	getTypeOutputInfo(node->consttype,
					  &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, node->constvalue);

	if (context->has_arrow)
	{
		switch (node->consttype)
		{
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case OIDOID:
			case FLOAT4OID:
			case FLOAT8OID:
			case NUMERICOID:
				{
					/* JSON array */
					appendStringInfoString(buf, extval);
					break;
				}
			default:
				{
					/* JSON attribute */
					appendStringInfo(buf, "\"%s\"", extval);
				}
		}
	}
	else
	{
		switch (node->consttype)
		{
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case OIDOID:
			case FLOAT4OID:
			case FLOAT8OID:
			case NUMERICOID:
				appendStringInfoString(buf, extval);
				break;
			case BOOLOID:
				if (strcmp(extval, "t") == 0)
					appendStringInfoString(buf, "true");
				else
					appendStringInfoString(buf, "false");
				break;
			case JSONOID:
			case JSONBOID:
			{
				json_t *root;
				json_error_t error;

				root = json_loads(extval, JSON_DECODE_ANY, &error);

				if (root == NULL)
					elog(ERROR, "dynamodb_fdw: Failed to parse the JSON value");

				dynamodb_deparse_json_value(root, buf);
				break;
			}
			default:
				dynamodb_deparse_string_literal(buf, extval);
				break;
		}
	}

	pfree(extval);

}

/*
 * dynamodb_deparse_func_expr
 *
 * Deparse a function call.
 */
static void
dynamodb_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	char		*proname;
	Expr		*arg;

	proname = get_func_name(node->funcid);

	/* check NULL for proname */
	if (proname == NULL)
		elog(ERROR, "dynamodb_fdw: cache lookup failed for function %u", node->funcid);
	
	/* Append the function name */
	appendStringInfo(buf, "%s(", proname);

	/* SIZE function just have only one argument */
	Assert(list_length(node->args) == 1);

	/* Deparse argument */
	arg = (Expr *) linitial(node->args);
	dynamodb_deparse_expr(arg, context);
	appendStringInfoChar(buf, ')');
}

/*
 * dynamodb_deparse_op_expr
 *
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
dynamodb_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Form_pg_operator form;
	ListCell   *arg;
	bool		is_array = false;
	Oid			consttype;
	DynamoDBOperatorsSupport	opkind;
	Expr	   *rightop;

	form = dynamodb_get_operator_expression(node->opno);

	/* Sanity check. */
	Assert(form->oprkind == 'b' && list_length(node->args) == 2);
	opkind = dynamodb_validate_operator_name(form);

	if (opkind == OP_JSON)
		context->has_arrow = true;
	else
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	arg = list_head(node->args);
	dynamodb_deparse_expr((Expr *) lfirst(arg), context);

	/* If right operand is a number, it means JSON array */
	arg = list_tail(node->args);
	rightop = (Expr *) lfirst(arg);
	if (opkind == OP_JSON)
	{
		Assert(nodeTag(rightop) == T_Const);
		consttype = ((Const *)rightop)->consttype;
		if (consttype == INT2OID ||
			consttype == INT4OID ||
			consttype == INT8OID)
			is_array = true;
	}
	else
		appendStringInfoChar(buf, ' ');

	if (is_array)
		appendStringInfoChar(buf, '[');
	else
		/* Deparse operator name. */
		dynamodb_deparse_operator_name(buf, form);

	/* Reset value of has_arrow */
	if (opkind == OP_JSON)
		context->has_arrow = true;
	else
	{
		context->has_arrow = false;
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse right operand. */
	dynamodb_deparse_expr(rightop, context);

	if (is_array)
		appendStringInfoChar(buf, ']');

	if (opkind != OP_JSON)
		appendStringInfoChar(buf, ')');
}

/*
 * dynamodb_deparse_operator_name
 *
 * Print the name of an operator.
 */
static void
dynamodb_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
{
	char	   *opname;

	/* opname is not a SQL identifier, so we should not quote it. */
	opname = dynamodb_replace_operator(NameStr(opform->oprname));

	/* Just print operator name. */
	appendStringInfoString(buf, opname);
}

/*
 * dynamodb_deparse_scalar_array_op_expr
 *
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void dynamodb_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
									 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Form_pg_operator form;
	Expr	   *arg1;
	Expr	   *arg2;
	char	   *opname;
	bool		useIn = false;

	form = dynamodb_get_operator_expression(node->opno);
	opname = NameStr(form->oprname);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Using IN clause for '= ANY' and NOT IN clause for '<> ALL' */
	if ((strcmp(opname, "=") == 0 && node->useOr == true) ||
		(strcmp(opname, "<>") == 0 && node->useOr == false))
		useIn = true;

	/* Get left and right argument for deparsing */
	arg1 = (Expr *) linitial(node->args);
	arg2 = (Expr *) lsecond(node->args);

	if (useIn)
	{
		/* Deparse left operand. */
		dynamodb_deparse_expr(arg1, context);
		appendStringInfoChar(buf, ' ');

		/* Add IN clause */
		if (strcmp(opname, "<>") == 0)
		{
			appendStringInfoString(buf, "NOT IN (");
		}
		else if (strcmp(opname, "=") == 0)
		{
			appendStringInfoString(buf, "IN (");
		}
	}

	arg2 = (Expr *) lsecond(node->args);
	switch (nodeTag((Node *) arg2))
	{
		case T_Const:
			{
				Const	   *c = (Const *) arg2;
				Oid			typoutput;
				bool		typIsVarlena;
				char	   *extval;
				bool		isstr = false;
				const char *valptr;
				bool		deparseLeft = true;
				unsigned int i = 0;
				bool		strstart = false;

				if (c->constisnull)
				{
					appendStringInfoString(buf, " NULL");
					return;
				}

				getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);
				extval = OidOutputFunctionCall(typoutput, c->constvalue);

				/* Determine array type */
				switch (c->consttype)
				{
					case INT2ARRAYOID:
					case INT4ARRAYOID:
					case INT8ARRAYOID:
					case FLOAT4ARRAYOID:
					case FLOAT8ARRAYOID:
					case NUMERICARRAYOID:
						isstr = false;
						break;
					default:
						isstr = true;
						break;
				}

				for (valptr = extval; *valptr; valptr++)
				{
					char		ch = *valptr;

					if (useIn)
					{
						if (i == 0 && isstr)
							appendStringInfoChar(buf, '\'');
					}
					else if (deparseLeft)
					{
						/* Deparse left operand. */
						dynamodb_deparse_expr(arg1, context);
						/* Append operator */
						appendStringInfo(buf, " %s ", opname);
						if (isstr)
							appendStringInfoChar(buf, '\'');
						deparseLeft = false;
					}

					if ((ch == '\"') && isstr)
					{
						if (strstart)
							strstart = false;
						else
							strstart = true;
					}

					/*
					 * Remove '{', '}' and \" character from the string.
					 * Because this syntax is not recognize by the remote
					 * DynamoDB server.
					 */
					if ((ch == '{' && i == 0) || (ch == '}' && (i == (strlen(extval) - 1))) || ch == '\"')
					{
						i++;
						continue;
					}

					if (ch == ',')
					{
						if (strstart && isstr)
							appendStringInfoChar(buf, ch);
						else if (useIn)
						{
							if (isstr)
								appendStringInfoChar(buf, '\'');
							appendStringInfoChar(buf, ch);
							appendStringInfoChar(buf, ' ');
							if (isstr)
								appendStringInfoChar(buf, '\'');
						}
						else
						{
							if (isstr)
								appendStringInfoChar(buf, '\'');
							if (node->useOr)
								appendStringInfoString(buf, " OR ");
							else
								appendStringInfoString(buf, " AND ");
							deparseLeft = true;
						}
						i++;
						continue;
					}
					appendStringInfoChar(buf, ch);

					i++;
				}

				if (isstr)
						appendStringInfoChar(buf, '\'');
			}
			break;
		case T_ArrayExpr:
			{
				bool		first = true;
				ListCell   *lc;

				foreach(lc, ((ArrayExpr *) arg2)->elements)
				{
					if (!first)
					{
						if (useIn)
						{
							appendStringInfoString(buf, ", ");
						}
						else
						{
							if (node->useOr)
								appendStringInfoString(buf, " OR ");
							else
								appendStringInfoString(buf, " AND ");
						}
					}

					if (useIn)
					{
						dynamodb_deparse_expr((Expr *) lfirst(lc), context);
					}
					else
					{
						/* Deparse left argument */
						appendStringInfoChar(buf, '(');
						dynamodb_deparse_expr(arg1, context);

						appendStringInfo(buf, " %s ", opname);

						/* Deparse each element in right argument */
						dynamodb_deparse_expr((Expr *) lfirst(lc), context);
						appendStringInfoChar(buf, ')');
					}
					first = false;
				}
				break;
			}
		default:
			elog(ERROR, "dynamodb_fdw: unsupported expression type for deparse: %d", (int) nodeTag(node));
			break;
	}

	/* Close IN clause */
	if (useIn)
		appendStringInfoChar(buf, ')');
}

/*
 * dynamodb_deparse_bool_expr
 *
 * Deparse a BoolExpr node.
 */
static void
dynamodb_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfoString(buf, "(NOT ");
			dynamodb_deparse_expr((Expr *) linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		dynamodb_deparse_expr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * dynamodb_deparse_null_test
 *
 * Deparse IS [NOT] NULL expression.
 */
static void
dynamodb_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	dynamodb_deparse_expr(node->arg, context);

	/* DynamoDB FDW only support IS [NOT] NULL */
	Assert((!node->argisrow) && !type_is_rowtype(exprType((Node *) node->arg)));

	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
}

/*
 * dynamodb_get_operator_expression
 *
 * Look up the operator based on the oid
 */
Form_pg_operator
dynamodb_get_operator_expression(Oid oid)
{
	HeapTuple			tuple;
	Form_pg_operator	form;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "dynamodb_fdw: cache lookup failed for operator %u", oid);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	ReleaseSysCache(tuple);

	return form;
}

/*
 * dynamodb_replace_operator
 *
 * Return the DynamoDB equivalent operator name
 */
static char *
dynamodb_replace_operator(char *in)
{
	if (strcmp(in, "!=") == 0)
		return (char *) "<>";
	else if (strcmp(in, "->>") == 0 || strcmp(in, "->") == 0)
		return (char *) ".";
	else
		return in;
}

/*
 * deparse remote INSERT statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by WITH CHECK OPTION
 */
void
dynamodb_deparse_insert(StringInfo buf, RangeTblEntry *rte,
				 		Index rtindex, Relation rel,
						List *targetAttrs, List **retrieved_attrs)
{
	AttrNumber	pindex;
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "INSERT INTO ");
	dynamodb_deparse_relation(buf, rel);
	appendStringInfoString(buf, " VALUE ");
	if (targetAttrs)
	{
		appendStringInfoString(buf, "{'");
		pindex = 1;
		first = true;
		foreach(lc, targetAttrs)
		{
			int	attnum = lfirst_int(lc);

			if (!first)
				appendStringInfoString(buf, ", '");
			first = false;
			if (pindex )
			dynamodb_deparse_column_ref(buf, rtindex, attnum, rte, retrieved_attrs, false);
			appendStringInfoString(buf, "' : ");
			appendStringInfoString(buf, "?");
			pindex++;
		}
		appendStringInfoString(buf, "};");
	}
}

/*
 * deparse remote DELETE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
dynamodb_deparse_delete(StringInfo buf, RangeTblEntry *rte,
				 		Index rtindex, Relation rel,
				 		List *returningList,
						List **retrieved_attrs)
{
	Oid				relid = RelationGetRelid(rel);
	dynamodb_opt   *opt = dynamodb_get_options(relid);
	char		   *partition_key= opt->svr_partition_key;
	char		   *sort_key= opt -> svr_sort_key;

	appendStringInfoString(buf, "DELETE FROM ");
	dynamodb_deparse_relation(buf, rel);

	
	appendStringInfo(buf, " WHERE %s = ? ", partition_key);

	if (!IS_KEY_EMPTY(sort_key))
		appendStringInfo(buf, "AND %s = ? ", sort_key);
	
	/* which is returned to *retrieved_attrs */
	dynamodb_deparse_returning_list(buf, rte, rtindex, rel,
									rel->trigdesc && rel->trigdesc->trig_delete_after_row,
									NIL, returningList, retrieved_attrs, true);
}

void
dynamodb_deparse_update(StringInfo buf, RangeTblEntry *rte,
							 Index rtindex, Relation rel,
							 List *targetAttrs,
							 List *withCheckOptionList, List *returningList,
							 List **retrieved_attrs)
{
	ListCell	   *lc;
	Oid				relid = RelationGetRelid(rel);
	dynamodb_opt   *opt = dynamodb_get_options(relid);
	char		   *partition_key = opt->svr_partition_key;
	char		   *sort_key = opt -> svr_sort_key;

	appendStringInfoString(buf, "UPDATE ");
	dynamodb_deparse_relation(buf, rel);

	foreach(lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);

		appendStringInfoString(buf, " SET ");

		dynamodb_deparse_column_ref(buf, rtindex, attnum, rte, retrieved_attrs, true);
		appendStringInfo(buf, " = ?");
	}

	appendStringInfo(buf, " WHERE \"%s\" = ? ", partition_key);

	if (!IS_KEY_EMPTY(sort_key))
		appendStringInfo(buf, "AND \"%s\" = ?", sort_key);

	dynamodb_deparse_returning_list(buf, rte, rtindex, rel,
									rel->trigdesc && rel->trigdesc->trig_delete_after_row,
									NIL, returningList, retrieved_attrs, false);
}

/*
 * dynamodb_validate_operator_name.
 *
 * Validate operator as supported or not.
 * Classify kinds of supported operator.
 */
DynamoDBOperatorsSupport
dynamodb_validate_operator_name(Form_pg_operator opform)
{
	char	   *opname;
	int			i=0;

	opname = NameStr(opform->oprname);
	/* Check if operator is supported comparison operator */
	for (i = 0; compOpName[i] != NULL; i++)
	{
		if (strcmp(opname, compOpName[i]) == 0)
			return OP_CONDITIONAL;
	}

	/* Check if operator is supported arrow operator */
	for (i = 0; jsonOpName[i] != NULL; i++)
	{
		if (strcmp(opname, jsonOpName[i]) == 0)
			return OP_JSON;
	}

	/* Funtion does not in supported lists*/
	return OP_UNSUPPORT;
}

/*
 * dynamodb_tlist_has_json_arrow_op.
 *
 * Determine whether target list has Jsonb arrow operator
 * that is safe to pushdown.
 */
bool
dynamodb_tlist_has_json_arrow_op(PlannerInfo *root, RelOptInfo *baserel, List *tlist)
{
	DynamoDBOperatorsSupport opkind;
	List	 *input_tlist;
	ListCell *lc;
	bool	 json_op_safe = false;
	List	 *att_list = NIL;
	ListCell *attlc;

	if (!IS_SIMPLE_REL(baserel))
		return false;

	input_tlist = (tlist) ? tlist : baserel->reltarget->exprs;

	/* Check arrow operator "->" and "->>" */
	foreach(lc, input_tlist)
	{
		Node *node = (Node *) lfirst(lc);
		char *attr_name;

		if (IsA(node, TargetEntry))
			node = (Node *) ((TargetEntry *) node)->expr;

		/*
		 * If any target expression is not pushdown, then we cannot
		 * push down Json arrow operator to the foreign server.
		 */
		if (!dynamodb_is_foreign_expr(root, baserel, (Expr *)node))
			return false;

		if (IsA(node, OpExpr))
		{
			OpExpr *oe = (OpExpr *) node;
			Form_pg_operator opform;

			opform = dynamodb_get_operator_expression(oe->opno);
			opkind = dynamodb_validate_operator_name(opform);

			if (opkind == OP_JSON)
				json_op_safe = true;
			else
				return false;
		}

		/*
		 * DynamoDB does not support selecting multiple attributes with the same name.
		 * Therefore, do not push down if there are duplicate attribute names.
		 */
		attr_name = dynamodb_get_attribute_name(node, root);

		foreach(attlc, att_list)
		{
			char *target_name = strVal(lfirst(attlc));

			if (strcmp(target_name, attr_name) == 0)
				return false;
		}
		att_list = lappend(att_list, makeString(attr_name));
	}

	return json_op_safe;
}

/*
 * dynamodb_store_attr_info
 *
 * Store the attribute name and the attribute number
 * in order that attributes appeared in target list.
 */
static void
dynamodb_store_attr_info(const char *col_name, int varno, List **retrieved_attr)
{
	attr_entry *attr = NULL;

	if (col_name == NULL)
		return;

	/* Store Attributes information in attribute list */
	attr = (attr_entry *) palloc(sizeof(attr_entry));
	attr->attrname = col_name;
	attr->attrno = varno;

	*retrieved_attr = lappend(*retrieved_attr, attr);
}

/*
 * dynamodb_get_column_name
 *
 * Get column name using column_name option or attribute_name
 */
static char *
dynamodb_get_column_name(Oid relid, int varattno)
{
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name
	 * FDW option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_attname(relid, varattno, false);

	return colname;
}

/*
 * dynamodb_get_document_path
 *
 * Get the document path that will be built to send to DynamoDB
 */
void dynamodb_get_document_path(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, Expr *expr)
{
	deparse_expr_cxt context;
	List	   *attrs_list;

	/* Fill portions of context common to upper, join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.scanrel = rel;
	context.has_arrow = false;
	context.attrs_list = &attrs_list;

	dynamodb_deparse_expr(expr, &context);
}
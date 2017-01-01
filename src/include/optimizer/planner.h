/*-------------------------------------------------------------------------
 *
 * planner.h
 *	  prototypes for planner.c.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

/* Expression kind codes for preprocess_expression */
#define EXPRKIND_QUAL			0
#define EXPRKIND_TARGET			1
#define EXPRKIND_RTFUNC			2
#define EXPRKIND_RTFUNC_LATERAL 3
#define EXPRKIND_VALUES			4
#define EXPRKIND_VALUES_LATERAL 5
#define EXPRKIND_LIMIT			6
#define EXPRKIND_APPINFO		7
#define EXPRKIND_PHV			8

/* Hook for plugins to get control in planner() */
typedef PlannedStmt *(*planner_hook_type) (Query *parse,
													   int cursorOptions,
												  ParamListInfo boundParams);
extern PGDLLIMPORT planner_hook_type planner_hook;


extern PlannedStmt *planner(Query *parse, int cursorOptions,
		ParamListInfo boundParams);

extern PlannedStmt *online_planner_hook(Query *parse, int cursorOptions,
				 ParamListInfo boundParams);

/*Online Query Planner*/
extern PlannedStmt *online_planner(Query *parse, int cursorOptions,
				 ParamListInfo boundParams);
extern PlannedStmt *standard_planner(Query *parse, int cursorOptions,
				 ParamListInfo boundParams);

extern Plan *subquery_planner(PlannerGlobal *glob, Query *parse,
				 PlannerInfo *parent_root,
				 bool hasRecursion, double tuple_fraction,
				 PlannerInfo **subroot);

extern void add_tlist_costs_to_plan(PlannerInfo *root, Plan *plan,
						List *tlist);

extern bool is_dummy_plan(Plan *plan);

extern Expr *expression_planner(Expr *expr);

extern Expr *preprocess_phv_expression(PlannerInfo *root, Expr *expr);

extern bool plan_cluster_use_sort(Oid tableOid, Oid indexOid);

extern Node *preprocess_expression(PlannerInfo *root, Node *expr, int kind);

extern void preprocess_qual_conditions(PlannerInfo *root, Node *jtnode);
#endif   /* PLANNER_H */

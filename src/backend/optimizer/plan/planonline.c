/*----------------------------------------------------------------------------
 *
 * planonline.c
 *		Online sample join optimizer.
 *
 * Copyright (c) 2015-2017, InitialD Lab
 *
 * IDENTIFICATION
 *     src/backend/optimizer/plan/planonline.c
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "access/nbtree.h"

#define MAX_PARSE_WITHTIME_VALUE (0xFFFFFFFFu)

#define _po_swap_scalar(type, x, y)	\
	do {	\
		type __po_tmp = x;	\
		x = y;	\
		y = __po_tmp;	\
	} while(0)

#define _po_fix_opexpr_larg_varno_attno(opexpr, new_varno, new_varattno)	\
	do {	\
		OpExpr *__po_opexpr = (opexpr);	\
		Var *__po_var;	\
			\
		Assert(IsA(linitial(__po_opexpr->args), Var));	\
		__po_var = (Var *) linitial((__po_opexpr->args));	\
		__po_var->varnoold = __po_var->varno;	\
		__po_var->varoattno = __po_var->varattno;	\
		__po_var->varno = (new_varno);	\
		__po_var->varattno = (new_varattno);	\
	} while(0)

#define _po_recover_opexpr_larg_varno_attno(opexpr)	\
	do {	\
		OpExpr *__po_opexpr = (opexpr);	\
		Var *__po_var;	\
			\
		Assert(IsA(linitial(__po_opexpr->args), Var));	\
		__po_var = (Var *) linitial((__po_opexpr->args));	\
		__po_var->varno = __po_var->varattno;	\
		__po_var->varattno = __po_var->varoattno;	\
	} while(0)

#define _po_fix_opexpr_funcid(opexpr)	\
	do {	\
		OpExpr *__po_opexpr = (opexpr);	\
			\
		if (!OidIsValid(__po_opexpr->opfuncid)) {	\
			__po_opexpr->opfuncid = get_opcode(__po_opexpr->opno);	\
		}	\
	} while(0)

typedef struct _po_flatten_expr_and_fix_ref_context {
	List	*var_list;
	int		ngrpcols;
} _po_flatten_expr_and_fix_ref_context;

typedef struct _po_find_rel_set_context{
	uint32	n;
	Bitmapset *set;
} _po_find_rel_set_context;

typedef struct _po_build_online_join_path_rel_info {
	Oid	 relid;
	Oid  primary_indexid;
	IndexOptInfo *primaryindex_info;
	List *join_quals;
	List *join_quals_indexid;
	Bitmapset *outedges; /* outbound edges */
	Bitmapset *inedges; /* inbound edges */
	uint32 outdegree;	/* out-degree */
	int joinseq;	/* -1 if it has not been added to the join path */

	List *comminfo; /* list of commutable quals' info */

	RelOptInfo *reloptinfo;
} _po_build_online_join_path_rel_info;

typedef struct _po_path_elem_info {
	int		relno;
	int		outdeg;
	Oid		join_qual_indexid;
} _po_path_elem_info;

typedef struct _po_path_info {
	int					len;
	_po_path_elem_info	*eleminfo;
} _po_path_info;

#define _po_comm_qual_info_get_other_varno(info, myvarno) \
	(((myvarno) == info->larg_varno) ? (info->rarg_varno) : (info->larg_varno))

#define _po_comm_qual_info_get_my_indexid(info, myvarno) \
	(((myvarno) == info->larg_varno) ? (info->larg_indexid) : (info->rarg_indexid))

typedef struct _po_build_online_join_path_commutable_qual_info {
	OpExpr	*qual;
	int		larg_varno;
	int		rarg_varno;
	Oid		larg_indexid;
	Oid		rarg_indexid;
	Oid		commute_opno;
} _po_build_online_join_path_commutable_qual_info;

typedef struct _po_fix_grp_expr_refs_context {
	List	*grpCols;
} _po_fix_grp_expr_refs_context;

typedef struct _po_find_relno_with_largest_joinseq_context {
	_po_build_online_join_path_rel_info *relinfo;
	int res_relno;
} _po_find_relno_with_largest_joinseq_context;


static Plan *_po_do_online_plan(PlannerGlobal *global, Query *parse,
							PlannerInfo  **subroot);
static void _po_check_if_all_ordinary_relations(Query *parse);
static void _po_check_if_all_rangetblref(Query *parse);
static void _po_check_if_no_inherited_table(Query *parse);
static Plan *_po_create_online_plan(PlannerInfo *root);
static List *_po_create_tlist_for_agg_and_join(PlannerInfo *root,
											  OnlineAgg *agg);
/* static Oid _po_get_to_numeric_procid(Oid typeid); */
static Expr *_po_make_online_agg_count(Const *c);
static Expr *_po_make_online_agg_sum(Expr *arg);
static void _po_flatten_expr_and_fix_ref(Expr *expr, List **p_var_list, int ngrpcols);
static bool _po_flatten_expr_and_fix_ref_impl(Node *node, _po_flatten_expr_and_fix_ref_context *cxt);
static Node *_po_fix_grp_expr_refs(Node *expr, List *grpCols);
static Node *_po_fix_grp_expr_refs_impl(Node *node, _po_fix_grp_expr_refs_context *ctx);
static void _po_build_online_join_path(PlannerInfo *root, OnlineAgg *agg, List *flat_tlist, bool adaptive);
static Bitmapset *_po_find_rel_set(Node *node, uint32 n);
static bool _po_find_rel_set_impl(Node *node, _po_find_rel_set_context *cxt);
static int _po_get_varno_if_is_var(Node *node);
static Oid _po_find_btindex_on_single_attr(RelOptInfo *reloptinfo, AttrNumber attrno);
static IndexOptInfo *_po_find_primary_index(List *index_opts);
#define _po_fix_onlinesamplejoin_refs(expr, relinfo) \
	if ((expr) != NULL) _po_fix_onlinesamplejoin_refs_impl((Node *) expr, relinfo)
static bool _po_fix_onlinesamplejoin_refs_impl(Node *expr, _po_build_online_join_path_rel_info *relinfo);
static int _po_find_relno_with_largest_joinseq(Node *expr, _po_build_online_join_path_rel_info *relinfo);
static bool _po_find_relno_with_largest_joinseq_impl(Node *expr, _po_find_relno_with_largest_joinseq_context *ctx);
static uint64 _po_get_count_from_btree(Oid indexid, List *quals) __attribute__((unused));

/* entry point of online sample join plugin */
PlannedStmt *online_planner_hook(Query *parse, int cursorOptions, 
								 ParamListInfo boundParams) {
	if (parse->hasOnline) {
		return online_planner(parse, cursorOptions, boundParams);
	}
	else {
		return standard_planner(parse, cursorOptions, boundParams);
	}
}

/* 
 * Generate online sample join plan for the query.
 *
 * NOTE we only support simple queries in the form as follow:
 *		SELECT [ADAPTIVE] [ONLINE] (expr of grouping cols|AGG(expr)) 
 *			[, (exprs of grouping cols|AGG(expr))]
 *		FROM REL1 [, RELi]*
 *		WHERE <join conditions>
 *		WITHTIME <time limit in ms>
 *		CONFIDENCE <confidence interval in percentage>
 *		REPORTINTERVAL <report interval in ms>
 *		GROUP BY col1 [, col]*;
 * 
 * It can also be used in a DECLARE CURSOR statement.
 *
 * */
PlannedStmt *online_planner(Query *parse, int cursorOptions,
							ParamListInfo boundParams) {
	PlannedStmt *result;
	Plan *top_plan;
	PlannerGlobal *glob;
	PlannerInfo *root;
	
	if (parse->utilityStmt) {
		if (!IsA(parse->utilityStmt, DeclareCursorStmt)) {
			ereport(ERROR, (errmsg("online planner does not support utility statement other than DECLARE CURSOR")));
		}
		cursorOptions |= ((DeclareCursorStmt *) parse->utilityStmt)->options;
	}

	/* set up PlannerGlobal */
	glob = makeNode(PlannerGlobal);

	glob->boundParams = boundParams;
	glob->subplans =
	glob->subroots = NIL;
	glob->rewindPlanIDs = NULL;
	glob->finalrtable =
	glob->finalrowmarks =
	glob->resultRelations =
	glob->relationOids =
	glob->invalItems = NIL;
	glob->nParamExec = 0;
	glob->lastPHId = 0;
	glob->lastRowMarkId = 0;
	glob->transientPlan = false;

	/* PLAN */
	top_plan = _po_do_online_plan(glob, parse, &root);

	if (cursorOptions & CURSOR_OPT_SCROLL) {
		ereport(ERROR, (errmsg("online planner does not support SCROLL option for DECLARE CURSOR")));
	}

	/* instead of set_plan_references, we fix the references
	 * with our own routines in the previous call */

	result = makeNode(PlannedStmt);
	result->commandType = parse->commandType;
	result->queryId = parse->queryId;
	result->hasReturning = false;
	result->hasModifyingCTE = false;
	result->canSetTag = parse->canSetTag;
	result->transientPlan = false;

	result->hasOnline = parse->hasOnline;
	result->withTime = parse->withTime;
	result->confidence = parse->confidence / 100.0;
	result->reportInterval = parse->reportInterval;
	result->nInitSamples = parse->nInitSamples;

	result->planTree = top_plan;
	result->rtable = glob->finalrtable;
	result->resultRelations = NIL;
	result->utilityStmt = parse->utilityStmt;
	result->subplans = NIL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = NIL;
	result->relationOids = glob->relationOids;
	result->invalItems = glob->invalItems;
	result->nParamExec = glob->nParamExec;
	
	return result;
}

static Plan *
_po_do_online_plan(PlannerGlobal *glob, Query *parse,
					 PlannerInfo **subroot) {
	PlannerInfo *root;
	Plan *plan;
	
	if (parse->commandType != CMD_SELECT) {
		ereport(ERROR, (errmsg("invalid syntax")));
	}

	if (parse->withTime > MAX_PARSE_WITHTIME_VALUE) {
		ereport(ERROR, (errmsg("time limit is too large %u", parse->withTime),
						errhint("time limit cannot exceed %u", MAX_PARSE_WITHTIME_VALUE)));
	}

	if (parse->confidence > 100) {
		ereport(ERROR, (errmsg("invalid confidence interval %u%%", parse->confidence)));
	}
	
	if (parse->reportInterval > MAX_PARSE_WITHTIME_VALUE) {
		ereport(ERROR, (errmsg("report interval is too large %u", parse->reportInterval),
						errhint("report interval cannot exceed %u", MAX_PARSE_WITHTIME_VALUE)));
	}

	/* Create a PlannerInfo data structure for this subquery */
	root = makeNode(PlannerInfo);
	root->parse = parse;
	root->glob = glob;
	root->query_level = 1;
	root->parent_root = NULL;
	root->plan_params = NIL;
	root->planner_cxt = CurrentMemoryContext;
	root->init_plans = NIL;
	root->cte_plan_ids = NIL;
	root->eq_classes = NIL;
	root->append_rel_list = NIL;
	root->rowMarks = NIL;
	root->hasInheritedTarget = false;
	
	if (parse->hasRecursive) {
		ereport(ERROR, (errmsg("online sample join does not support recursion")));
	}
	root->hasRecursion = false;
	root->wt_param_id = -1;
	root->non_recursive_plan = NULL;

	if (parse->cteList) {
		ereport(ERROR, (errmsg("online sample join does not support WITH list")));
	}
	
	if (parse->hasSubLinks) {
		ereport(ERROR, (errmsg("online sample join does not support sublinks")));
	}
	
	/* check whether all relations in the rtable are ordinary relation */
	_po_check_if_all_ordinary_relations(parse);
	
	/* check wheterh all terms in the FromExpr are RangeTblRef */
	_po_check_if_all_rangetblref(parse);
	
	if (parse->setOperations) {
		ereport(ERROR, (errmsg("online sample join does not support set operations")));
	}

	root->hasJoinRTEs = false;
	root->hasLateralRTEs = false;

	if (parse->rowMarks) {
		ereport(ERROR, (errmsg("online sample join does not allow row marks")));
	}
	
	/* no support for inheritance */
	_po_check_if_no_inherited_table(parse);

	if (parse->havingQual != NULL) {
		ereport(ERROR, (errmsg("online sample join does not support having clause")));;
	}
	root->hasHavingQual = false;

	root->hasPseudoConstantQuals = false;

	/*
	if (parse->groupClause != NULL) {
		ereport(ERROR, (errmsg("online sample join does not support group by clause")));
	} */
	
	/* preprocess of expressions in target lists and quals */
	parse->targetList = (List *) preprocess_expression(root, (Node *) parse->targetList,
													   EXPRKIND_TARGET);

	if (parse->withCheckOptions != NIL) {
		ereport(ERROR, (errmsg("online sample join does not support with check options")));
	}

	if (parse->returningList != NIL) {
		ereport(ERROR, (errmsg("online sample join does not support returning list")));
	}

	preprocess_qual_conditions(root, (Node *) parse->jointree);
	
	if (parse->windowClause != NIL) {
		ereport(ERROR, (errmsg("online sample join does not support window clause")));
	}

	if (parse->limitOffset != NULL) {
		ereport(ERROR, (errmsg("online sample join does not support OFFSET clause")));
	}

	if (parse->limitCount != NULL) {
		ereport(ERROR, (errmsg("online sample join does not support LIMIT clause")));
	}

	Assert(root->append_rel_list == NIL); /* ??? is it NIL? */

	/* no expressions in rte, 
	 * no having quals, 
	 * and no outer joins */

	if (parse->distinctClause != NIL) {
		ereport(WARNING, (errmsg("DISTINCT clause is ignored by online sample join")));
	}

	if (parse->sortClause != NIL) {
		ereport(WARNING, (errmsg("ORDER BY clause is ignored by online sample join")));
	}

	if (!parse->hasAggs) {
		ereport(ERROR, (errmsg("no aggregation in the target list of online sample join")));
	}

	plan = _po_create_online_plan(root);

	if (subroot)
		*subroot = root;

	return plan;
}

static void
_po_check_if_all_ordinary_relations(Query *parse) {
	ListCell *rt;

	foreach(rt, parse->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

		if (rte->rtekind != RTE_RELATION) {
			ereport(ERROR, (errmsg("range table %s is not ordinary relation",
								   rte->eref->aliasname)));
		}
	}
}

static void
_po_check_if_all_rangetblref(Query *parse) {
	FromExpr *jointree = parse->jointree;
	ListCell *jt;
	
	foreach(jt, jointree->fromlist) {
		Node *jtnode = lfirst(jt);

		if (!IsA(jtnode, RangeTblRef)) {
			ereport(ERROR, (errmsg("online sample join does not support JOIN syntax"),
							errhint("rewrite it in the form of SELECT ... FROM ... WHERE ...")));
		}
	}
}

static void 
_po_check_if_no_inherited_table(Query *parse) {
	ListCell *rl;

	foreach(rl, parse->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rl);
		
		/* TODO all items in rtable should be ordinary relation 
		 * since we've checked that. But it might not be true if we
		 * were to support subqueries someday. */
		Assert(rte->rtekind == RTE_RELATION);

		if (rte->inh) {
			if (has_subclass(rte->relid)) {
				ereport(ERROR, (errmsg("online sample join does not support inheritance")));
			}
			rte->inh = false;
		}
	}
}

static Plan *
_po_create_online_plan(PlannerInfo *root) {
	Query		*parse = root->parse;
	OnlineAgg	*online_agg_plan;
	List		*flat_tlist;

	
	Assert(!parse->limitCount && !parse->limitOffset);
	Assert(!parse->setOperations);
	Assert(!parse->rowMarks);
	Assert(parse->hasAggs);

	{
		/* don't need to preprocess the targetlist, 
		 * the security quals, windoe functions, 
		 * subplan target lists */
	

		/* make online agg plan */
		online_agg_plan = makeNode(OnlineAgg);

		/* 
		 * sample_join_plan's tlist contains TargetEntries of vars in OnlineAgg...'s or
		 * group columns
		 *
		 * agg_plan's tlist is a list of TargetEntries of OnlineAgg...'s or 
		 * non-junk group columns or expressions of group columns 
		 * 
		 * info of grp cols in online_agg_plan is also filled up in this proc.
		 */
		flat_tlist = _po_create_tlist_for_agg_and_join(root, online_agg_plan);

		/* build join path and fix references in the flat target list */
		_po_build_online_join_path(root, online_agg_plan, flat_tlist, parse->adaptive);
	}

	return (Plan *) online_agg_plan;
}

static int _po_get_grouping_colidx(List *grpcls, Index sgref, SortGroupClause **p_grpcl) {
	ListCell *cl;
	int colno = 0;

	foreach(cl, grpcls) {
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(cl);

		if (grpcl->tleSortGroupRef == sgref) {
			*p_grpcl = grpcl;
			return colno;
		}
		++colno;
	}

	return -1;
}

static List *
_po_create_tlist_for_agg_and_join(PlannerInfo *root,
								  OnlineAgg *agg) {
	Query		*parse = root->parse;
	List		*tlist = parse->targetList;
	List		*groupClause = parse->groupClause;
	List		*grpcols = NIL; /* start from 1 */
	int			ngrpcols = 0;
	List		*aggref_varlist = NIL; /* start from len(grpcols) + 1 */
	List		*grp_expr_tles = NIL; /* expressions in terms of gouping cols */
	List		*new_tlist = NIL;
	int			new_tlist_len = 0;
	List		*flat_tlist;
	int			flat_tlist_len;
	TargetEntry *new_tle;
	Index		grpidx;
	SortGroupClause *grpcl = NULL;
	ListCell	*tl;
	Aggref		*aggref;
	Var			*new_var;

	agg->numGrpCols = list_length(groupClause);
	if (agg->numGrpCols > 0) {
		agg->grpColsIdx = palloc(sizeof(AttrNumber) * agg->numGrpCols);
		agg->grpEqOps = palloc(sizeof(Oid) * agg->numGrpCols);
	}
	
	foreach(tl, tlist) {
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		
		if (tle->ressortgroupref) {
			/* add the tle to the subplan tlist */
			new_tle = flatCopyTargetEntry(tle);
			new_tle->resno = ++ngrpcols;
			grpcols = lappend(grpcols, new_tle);
			
			/* fill in the grp cols in agg */
			grpidx = _po_get_grouping_colidx(groupClause, tle->ressortgroupref, &grpcl);
			Assert(grpidx >= 0 && grpidx < agg->numGrpCols);
			agg->grpColsIdx[grpidx] = ngrpcols;
			agg->grpEqOps[grpidx] = grpcl->eqop;

			/* add non-junk group column to new tlist */
			if (!tle->resjunk) {
				
				new_var = makeVarFromTargetEntry(OUTER_VAR, new_tle);
				new_tle = flatCopyTargetEntry(tle);
				new_tle->expr = (Expr *) new_var;
				new_tle->resno = ++new_tlist_len;
				new_tlist = lappend(new_tlist, new_tle);
			}
		}

		else if (!IsA(tle->expr, Aggref)) {
			/* column that is expression of group column, nothing to add to flat tlist */
			Assert(!tle->resjunk);

			new_tle = flatCopyTargetEntry(tle);
			new_tle->resno = ++new_tlist_len;
			new_tlist = lappend(new_tlist, new_tle);
			
			/* can't fix the refs until all grouping cols are found */
			grp_expr_tles = lappend(grp_expr_tles, new_tle);
		}

		else {
			Expr *online_agg_info;
			aggref = (Aggref *) tle->expr;

			if (aggref->aggorder != NIL) {
				ereport(ERROR, (errmsg("online sample join does not support sort clause")));
			}

			if (aggref->aggdistinct != NIL) {
				ereport(ERROR, (errmsg("online sample join does not support distinct clause")));
			}

			if (aggref->aggfilter != NULL) {
				ereport(ERROR, (errmsg("online sample join does not support filter clause")));
			}

			if (aggref->aggkind != AGGKIND_NORMAL) {
				ereport(ERROR, (errmsg("online sample join does not support aggregation %d", aggref->aggfnoid)));
			}
			

			/* 1. find out how to convert the aggregation argument to numeric type if it is not count
			 * 2. flatten the argument expression */
			if (PG_AGG_IS_SUM(aggref->aggfnoid)) {
				Expr *arg;

				if (list_length(aggref->args) != 1) {
					ereport(ERROR, (errmsg("invalid argument to sum")));
				}
				arg = (Expr *) (((TargetEntry *) linitial(aggref->args))->expr);

				if (IsA(arg, Const)) {
					Const *c = (Const *) arg;
					online_agg_info = _po_make_online_agg_count(c);	
				}
				else {
					_po_flatten_expr_and_fix_ref(arg, &aggref_varlist, agg->numGrpCols);
					online_agg_info = _po_make_online_agg_sum(arg);
				}
			}

			else if (PG_AGG_IS_COUNT(aggref->aggfnoid)) {
				online_agg_info = _po_make_online_agg_count(NULL);
			}

			else {
				ereport(ERROR, (errmsg("online sample join does not support aggregation %d", aggref->aggfnoid)));
			}
			
			new_tle = flatCopyTargetEntry(tle);
			new_tle->expr = online_agg_info;
			new_tle->resno = ++new_tlist_len;
			new_tlist = lappend(new_tlist, new_tle);
		}
	}
	
	Assert(ngrpcols == agg->numGrpCols);
	
	/* fix references to grouping columns */
	foreach(tl, grp_expr_tles) {
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		
		tle->expr = (Expr *) _po_fix_grp_expr_refs((Node *) tle->expr, grpcols);
	}
	
	/* create flat_tlist */
	flat_tlist = grpcols;
	flat_tlist_len = ngrpcols;
	foreach(tl, aggref_varlist) {
		Var *var = (Var *) lfirst(tl);		
		new_tle = makeTargetEntry((Expr *) var, ++flat_tlist_len, NULL, false);
		flat_tlist = lappend(flat_tlist, new_tle);
	}

	agg->plan.targetlist = new_tlist;
	return flat_tlist;
}

static Node *
_po_fix_grp_expr_refs(Node *expr, List *grpCols) {
	_po_fix_grp_expr_refs_context ctx;

	ctx.grpCols = grpCols;
	return _po_fix_grp_expr_refs_impl(expr, &ctx);
}

static Node *
_po_fix_grp_expr_refs_impl(Node *node, _po_fix_grp_expr_refs_context *ctx) {
	ListCell *cl;	
	TargetEntry *tle;

	foreach(cl, ctx->grpCols) {
		tle = (TargetEntry *) lfirst(cl);	

		if (equal(node, tle->expr)) {
			Var *new_var = makeVarFromTargetEntry(OUTER_VAR, tle);
			return (Node *) new_var;
		}
	}

	return expression_tree_mutator(node, _po_fix_grp_expr_refs_impl, (void *) ctx);
}

static Expr *
_po_make_online_agg_count(Const *c) {
	OnlineAggCount *info;

	info = makeNode(OnlineAggCount);
	info->scale = c;

	return (Expr *) info;
}

static Expr *
_po_make_online_agg_sum(Expr *arg) {
	OnlineAggSum *info;
	Oid	argtype = exprType((const Node *) arg);

	Assert(OidIsValid(argtype));

	info = makeNode(OnlineAggSum);
	info->arg = arg;
	info->arg_typeid = argtype;

	return (Expr *) info;
}

static void
_po_flatten_expr_and_fix_ref(Expr *expr, List **p_var_list, int ngrpcols) {
	_po_flatten_expr_and_fix_ref_context ctx;
	
	ctx.var_list = NIL;
	ctx.ngrpcols = ngrpcols;

	_po_flatten_expr_and_fix_ref_impl((Node *) expr, &ctx);
	*p_var_list = ctx.var_list;
}

static bool
_po_flatten_expr_and_fix_ref_impl(Node *node, _po_flatten_expr_and_fix_ref_context *ctx) {
	
	if (IsA(node, Var)) {
		ListCell *cl;
		Var *var = (Var *) node;
		Var *other = NULL;
		int varidx = 0;
		
		foreach(cl, ctx->var_list) {
			Var *v = (Var *) lfirst(cl);
			++varidx;

			if (v->varno == var->varno &&
				v->varattno == var->varattno) {
				other = v;
				break;
			}
		}

		if (other == NULL) {
			++varidx;
			other = palloc(sizeof(Var));
			*other = *var;
			ctx->var_list = lappend(ctx->var_list, other);
		}
		
		var->varnoold = var->varno;
		var->varoattno = var->varoattno;
		var->varno = OUTER_VAR;
		var->varattno = ctx->ngrpcols + varidx;

		return false;
	}
	else if (IsA(node, Aggref)) {
		ereport(ERROR, (errmsg("unexpected aggregation")));	
	}

	return expression_tree_walker(node, _po_flatten_expr_and_fix_ref_impl, (void *) ctx);
}

static void
_po_build_online_join_path(PlannerInfo *root, OnlineAgg *agg, 
							List *flat_tlist, bool adaptive) {
	typedef _po_build_online_join_path_rel_info					_po_RelInfo;
	typedef _po_build_online_join_path_commutable_qual_info		_po_CommInfo;

	Query			*parse = root->parse;
	FromExpr		*fromexpr = parse->jointree;	
	int				rtable_length = list_length(parse->rtable);
	int				nrel = list_length(fromexpr->fromlist);
	Oid				*rtable_relid = ((Oid*) palloc(sizeof(Oid) * rtable_length)) - 1;
	RelOptInfo		**rtable_reloptinfo = ((RelOptInfo**) palloc(sizeof(RelOptInfo*) * rtable_length)) - 1;
	_po_RelInfo		*relinfo = ((_po_RelInfo *) palloc(sizeof(_po_RelInfo) * nrel)) - 1;
		/* quals in comminfo can be added to join_quals after we build the join path */
	int				i;
	ListCell		*lc;
	List			*candidate_paths = NIL; /* list of _po_path_info */
	List			*rel_quals = NIL;
	uint32			nplans;
	
	/* initialize relation Oids and edges */
	i = 1;
	foreach(lc, parse->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		RelOptInfo	*rel;
		rtable_relid[i] = rte->relid;

		rel = makeNode(RelOptInfo);
		rel->reloptkind = RELOPT_BASEREL;
		rel->relids = bms_make_singleton(rte->relid);
		rel->relid = rte->relid;
		rel->rtekind = rte->rtekind;
		get_relation_info(root, rte->relid, rte->inh, rel);

		rtable_reloptinfo[i++] = rel;
	}

	i = 1;
	foreach(lc, (List *) fromexpr->fromlist) {
		RangeTblRef *rtf = (RangeTblRef *) lfirst(lc);

		relinfo[i].relid = rtable_relid[rtf->rtindex];
		relinfo[i].join_quals = NIL;
		relinfo[i].join_quals_indexid = NIL;
		relinfo[i].outedges = bms_make_singleton(nrel + 1);
		relinfo[i].inedges = bms_make_singleton(nrel + 1);
		relinfo[i].outdegree = 0;
		relinfo[i].joinseq = -1;
		relinfo[i].comminfo = NIL;
		relinfo[i].reloptinfo = rtable_reloptinfo[rtf->rtindex];
		relinfo[i].primaryindex_info = _po_find_primary_index(relinfo[i].reloptinfo->indexlist);
		relinfo[i].primary_indexid = relinfo[i].primaryindex_info->indexoid;
		++i;
	}

	/* build join qual DAG: for qual A.x op f(B, C, ...),  there're edges from A to B, C, ... */
	foreach(lc, (List *) fromexpr->quals) {
		Node	*node = (Node *) lfirst(lc);
		
		if (IsA(node, OpExpr)) {
			OpExpr		*opexpr = (OpExpr *) node;
			int			nargs = list_length(opexpr->args);
			
			if (opexpr->opresulttype != BOOLOID) {
				ereport(ERROR, (errmsg("unexpected return type of join conditions")));
			}

			if (nargs == 1) {

				rel_quals = lappend(rel_quals, opexpr);
			}
			else {
				Node	*larg = (Node *) linitial(opexpr->args);
				Node	*rarg = (Node *) lsecond(opexpr->args);
				int		larg_varno = _po_get_varno_if_is_var(larg);
				int		rarg_varno = _po_get_varno_if_is_var(rarg);
				Oid		larg_indexid = InvalidOid;
				Oid		rarg_indexid = InvalidOid;
				Bitmapset *set;

				Assert(nargs == 2);

				do {
					if (larg_varno) {
						if (rarg_varno) {
							Oid		commute_opno;
							if (larg_varno == rarg_varno) {
								rel_quals = lappend(rel_quals, opexpr);
								break; /* break to end if (nargs == 1) */
							}

							commute_opno = get_commutator(opexpr->opno);	
							if (OidIsValid(commute_opno)) {

								/* the op is commutable, check if there's index */
								rarg_indexid = _po_find_btindex_on_single_attr(
													relinfo[rarg_varno].reloptinfo,
													((Var *) rarg)->varattno);
								if (OidIsValid(rarg_indexid)) {

									larg_indexid = _po_find_btindex_on_single_attr(
													relinfo[larg_varno].reloptinfo,
													((Var *) larg)->varattno);

									if (OidIsValid(larg_indexid)) {
										/* commutable and both side has index */	
										_po_CommInfo *c = palloc(sizeof(_po_CommInfo));
										c->qual = opexpr;
										c->larg_varno = larg_varno;
										c->rarg_varno = rarg_varno;
										c->larg_indexid = larg_indexid;
										c->rarg_indexid = rarg_indexid;
										c->commute_opno = commute_opno;

										relinfo[larg_varno].comminfo = lappend(relinfo[larg_varno].comminfo, c);
										relinfo[rarg_varno].comminfo = lappend(relinfo[rarg_varno].comminfo, c);
										break; /* break to end if (nargs == 1) */
									}
									else {

										_po_swap_scalar(Node *, larg, rarg);
										_po_swap_scalar(int, larg_varno, rarg_varno);
										_po_swap_scalar(Oid, larg_indexid, rarg_indexid);
	
										/* ??? need to make a copy? */
										linitial(opexpr->args) = larg;
										lsecond(opexpr->args) = rarg;
										opexpr->opno = commute_opno;
										opexpr->opfuncid = InvalidOid;
									}
								}
							}
							/* else op is not commutable */
						}	
					}
					else if (rarg_varno) {
						/* have to commute it */
						Oid		commute_opno = get_commutator(opexpr->opno);
						if (!OidIsValid(commute_opno)) {
							ereport(ERROR, (errmsg("online sample does not support general conditions")));
						}
	
						_po_swap_scalar(Node *, larg, rarg);
						_po_swap_scalar(int, larg_varno, rarg_varno);

						/* ??? make a copy? */
						linitial(opexpr->args) = larg;
						lsecond(opexpr->args) = rarg;
						opexpr->opno = commute_opno;
						opexpr->opfuncid = InvalidOid;
					}
					else {
						ereport(ERROR, (errmsg("online sample does not support general conditions")));
					}

					set = _po_find_rel_set(rarg, nrel);
					
					if (!OidIsValid(larg_indexid)) {
						/* need to look up btree index for left arg */
						larg_indexid = _po_find_btindex_on_single_attr(
										relinfo[larg_varno].reloptinfo,
										((Var *) larg)->varattno);
					}
					
					/* no index on the left or left relation appears on both sides*/
					if (larg_indexid == InvalidOid || bms_is_member(larg_varno, set)) { 
						/* rel qual */
						rel_quals = lappend(rel_quals, opexpr);

						bms_free(set);
					}
					else {
						/* join qual */
						if (!OidIsValid(larg_indexid)) {
							ereport(ERROR, (errmsg("cannot find btree index for column (%u, %u)",
											   relinfo[larg_varno].relid, ((Var *) larg)->varattno)));
						}
						relinfo[larg_varno].join_quals = lappend(relinfo[larg_varno].join_quals, opexpr);
						relinfo[larg_varno].join_quals_indexid = lappend_oid(relinfo[larg_varno].join_quals_indexid, larg_indexid);

						/* add the edge from larg_varno to all relations in the set */
						relinfo[larg_varno].outedges = bms_join(relinfo[larg_varno].outedges, set);
					}
				} while(0); 
			} /* end if (nargs == 1) */
			
		}
		/* TODO support RowCompareExpr */
		/*
		else if (IsA(node, RowCompareExpr)) {

		} */
		else {
			/* TODO support general qual */
			ereport(ERROR, (errmsg("online sample does not support join conditions other than B.x op A.y")));
		}
	}
	
	/* do topological sort */
	{
		List		*paths = NIL;	/* list of _po_path_elem_info */
		int			i;
		_po_path_info *cur_path, *next_path;

#define NEW_PATHINFO(varname, newlen)	\
		varname = (_po_path_info *) palloc(sizeof(_po_path_info));	\
		varname->eleminfo = ((_po_path_elem_info *) palloc(sizeof(_po_path_elem_info) * nrel)) - 1;	\
		varname->len = (newlen)

#define COPY_PATHINFO(from, to)	\
		NEW_PATHINFO(to, from->len);	\
		memcpy(to->eleminfo + 1, from->eleminfo + 1, sizeof(_po_path_elem_info) * nrel)

#define UPDATE_NEXT(varname, idx, indexid)	\
		do {	\
			int __idx = idx;	\
			int relno = varname->eleminfo[__idx].relno;	\
			int i;	\
			++varname->len;	\
			if (__idx != varname->len) {	\
				_po_path_elem_info t = varname->eleminfo[varname->len];	\
				varname->eleminfo[varname->len] = varname->eleminfo[__idx];	\
				varname->eleminfo[__idx] = t;	\
			}	\
			for (i = varname->len + 1; i <= nrel; ++i) {	\
				if (bms_is_member(varname->eleminfo[i].relno, relinfo[relno].inedges)) {	\
					--varname->eleminfo[i].outdeg;		\
				}	\
			}	\
			varname->eleminfo[varname->len].join_qual_indexid = indexid;	\
		} while (0)

#define DELETE_PATHINFO(varname)	\
		pfree(varname->eleminfo + 1);	\
		pfree(varname)


		/* calculate the outdegrees and inedges  */
		NEW_PATHINFO(cur_path, 0);
		for (i = 1; i <= nrel; ++i) {
			relinfo[i].outdegree = 0;
			
			bms_foreach(dst, relinfo[i].outedges) {
				Assert(dst <= nrel + 1);
				if (dst == nrel + 1) continue;
				
				++relinfo[i].outdegree;
				bms_add_member(relinfo[dst].inedges, i);
			} bms_endforeach()
			
			cur_path->eleminfo[i].relno = i;
			cur_path->eleminfo[i].outdeg = relinfo[i].outdegree;
			cur_path->eleminfo[i].join_qual_indexid = InvalidOid;
		}
		paths = lappend(paths, cur_path);
		
		while (paths != NIL) {
			bool new_path_found = false;
			Bitmapset *visited = bms_make_singleton(nrel + 1);
			Bitmapset *found_index = bms_make_singleton(nrel + 1);
			List	  *singlerel_quals_i = NIL;
			List	  *singlerel_quals_indexid = NIL;

			cur_path = (_po_path_info *) linitial(paths);
			paths = list_delete_first(paths);


			for (i = 1; i <= cur_path->len; ++i) {
				bms_add_member(visited, cur_path->eleminfo[i].relno);
			}
			
			/* 1. those that joins with previous rels and has degree 0 */
			for (i = cur_path->len + 1; i <= nrel; ++i) {
				_po_path_elem_info *curinfo = &(cur_path->eleminfo[i]);
				int relno = curinfo->relno;
				_po_RelInfo	*currelinfo = &relinfo[relno];
				List *considered_indexid = NIL;
				
				if (curinfo->outdeg != 0) continue;

				if (currelinfo->join_quals != NIL) {
					ListCell *lc, *lc2;
					
					forboth(lc, currelinfo->join_quals,
							lc2, currelinfo->join_quals_indexid) {
						OpExpr *opexpr = (OpExpr *) lfirst(lc);
						Node *rarg = (Node *) lsecond(opexpr->args);
						Bitmapset *set;
						bool issubset;
						Oid indexid = lfirst_oid(lc2);
						int nele;

						if (list_member_oid(considered_indexid, indexid)) {
							continue;
						}

						set = _po_find_rel_set(rarg, nrel);
						issubset = bms_is_subset(set, visited);
						nele = bms_num_members(set) - 1;
						bms_free(set);
						
						/* do not consider `attr op const' unless this is the first
						 * relation */
						if (issubset) {
							if (cur_path->len == 0 || nele > 0) {
								COPY_PATHINFO(cur_path, next_path);
								UPDATE_NEXT(next_path, i, indexid);
								if (next_path->len == nrel) {
									candidate_paths = lappend(candidate_paths, next_path);
								}
								else {
									paths = lappend(paths, next_path);
								}
								considered_indexid = lappend_oid(considered_indexid, indexid);
								new_path_found = true;
							}
							else {
								singlerel_quals_i = lappend_int(singlerel_quals_i, i);
								singlerel_quals_indexid = lappend_oid(singlerel_quals_indexid, indexid);
							}
						}
					}
				}

				if (currelinfo->comminfo != NIL) {
					ListCell *lc;

					foreach(lc, currelinfo->comminfo) {
						_po_CommInfo *cinfo = (_po_CommInfo *) lfirst(lc);

						int otherno = _po_comm_qual_info_get_other_varno(cinfo, relno);
						Oid indexid = _po_comm_qual_info_get_my_indexid(cinfo, relno);
						
						if (list_member_oid(considered_indexid, indexid)) {
							continue;
						}

						if (bms_is_member(otherno, visited)) {
							COPY_PATHINFO(cur_path, next_path);
							UPDATE_NEXT(next_path, i, indexid);
							if (next_path->len == nrel) {
								candidate_paths = lappend(candidate_paths, next_path);
							}
							else {
								paths = lappend(paths, next_path);
							}
							considered_indexid = lappend_oid(considered_indexid, indexid);
							new_path_found = true;
						}
					}
				}

				if (considered_indexid != NIL) {
					bms_add_member(found_index, relno);
				}
				list_free(considered_indexid);
			}

			/*
			 * 2. try `attr op const`
			 */
			if (!new_path_found || cur_path->len == 0) {
				ListCell *lc, *lc2;
				List* considered_indexid = NIL;
				int prev = -1;
				

				forboth(lc, singlerel_quals_i, lc2, singlerel_quals_indexid) {
					int i = lfirst_int(lc);
					Oid indexid = lfirst_oid(lc2);
					
					if (prev != i) {
						if (prev != -1) {
							if (considered_indexid != NIL) {
								bms_add_member(found_index, cur_path->eleminfo[prev].relno);
							}
						}
						list_free(considered_indexid);
						considered_indexid = NIL;
						prev = i;
					}

					if (bms_is_member(cur_path->eleminfo[i].relno, found_index)) {
						continue;
					}

					if (list_member_oid(considered_indexid, indexid)) {
						continue;
					}
					
					COPY_PATHINFO(cur_path, next_path);
					UPDATE_NEXT(next_path, i, indexid);
					if (next_path->len == nrel) {
						candidate_paths = lappend(candidate_paths, next_path);
					}
					else {
						paths = lappend(paths, next_path);
					}
					considered_indexid = lappend_oid(considered_indexid, indexid);
					new_path_found = true;
				}
				if (prev != -1) {
					if (considered_indexid != NIL) {
						bms_add_member(found_index, cur_path->eleminfo[prev].relno);
					}
				}
			}

			/* 3. if there's no new path found or this is the first relation, 
			 * choose those with degree 0 */
			if (!new_path_found || cur_path->len == 0)
			for (i = cur_path->len + 1; i <= nrel; ++i) {
				_po_path_elem_info *curinfo = &(cur_path->eleminfo[i]);
				
				if (curinfo->outdeg != 0) continue;

				/* this relation has already been considered in step 1 */
				if (bms_is_member(curinfo->relno, found_index)) continue;
				
				COPY_PATHINFO(cur_path, next_path);
				UPDATE_NEXT(next_path, i, InvalidOid);
				if (next_path->len == nrel) {
					candidate_paths = lappend(candidate_paths, next_path);
				}
				else {
					paths = lappend(paths, next_path);
				}
				new_path_found = true;
			}

			/* 
			 * 4. there're cycles, just ignore it for now
			 * if (!new_path_found) { } */

			DELETE_PATHINFO(cur_path);	
			bms_free(visited);
			bms_free(found_index);
			list_free(singlerel_quals_i);
			list_free(singlerel_quals_indexid);
		}
	}

	if (candidate_paths == NIL) {
		ereport(ERROR, (errmsg("cannot find any valid join path")));
	}

	/* add the commutable quals to join quals and output */	
	agg->candidate_join_plans = NIL;
	nplans = 0;
	foreach (lc, candidate_paths) {
		_po_path_info *cur_path = (_po_path_info *) lfirst(lc);
		OnlineSampleJoin *plan = makeNode(OnlineSampleJoin);
		int i;
		
		plan->no = nplans++;
		plan->adaptive = adaptive;
		plan->nrel = nrel;
		plan->relid = (Oid *) palloc(sizeof(Oid) * (nrel + 1));
		plan->indexid = (Oid *) palloc(sizeof(Oid) * (nrel + 1));
		plan->join_quals = (List **) palloc(sizeof(List *) * (nrel + 1));
		plan->primaryindex_info = (IndexOptInfo **) 
					palloc(sizeof(IndexOptInfo *) * (nrel+1));
		plan->primary_indexid = (Oid *) palloc(sizeof(Oid) * (nrel + 1));
		plan->rel_quals = (List **) palloc(sizeof(List *) * (nrel + 1));

		plan->relid[0] = InvalidOid;
		plan->indexid[0] = InvalidOid;
		plan->primary_indexid[0] = InvalidOid;
		plan->join_quals[0] = NIL;
		plan->rel_quals[0] = NIL;
		plan->primaryindex_info[0] = NULL;

		for (i = 1; i <= nrel; ++i) {
			int relno = cur_path->eleminfo[i].relno;
			relinfo[relno].joinseq = i;
			plan->relid[i] = relinfo[relno].relid;
			plan->indexid[i] = cur_path->eleminfo[i].join_qual_indexid;
			plan->primary_indexid[i] = relinfo[relno].primary_indexid;
			plan->primaryindex_info[i] = relinfo[relno].primaryindex_info;
			plan->join_quals[i] = NIL;
			plan->rel_quals[i] = NIL;
		}

		for (i = 1; i <= nrel; ++i) {
			ListCell *lc, *lc2;
			int relno = cur_path->eleminfo[i].relno;
			Oid indexid = plan->indexid[i];

			forboth(lc, relinfo[relno].join_quals,
					lc2, relinfo[relno].join_quals_indexid) {
				OpExpr *qual = (OpExpr *) copyObject(lfirst(lc));
				Oid	qual_indexid = lfirst_oid(lc2);

				/* don't need to worry about having relations after this because
				 * the relations are strictly topologically sorted,
				 *
				 * TODO which may not be true when we allow cycles. 
				 *		check if set \in leftrels 
				Node *rarg = (Node *) lsecond(qual->args);
				Bitsetmap *set;
				set = _po_find_rel_set(rarg, nrel); */

				if (qual_indexid == indexid) {
					plan->join_quals[i] = lappend(plan->join_quals[i], qual);
				}
				else {
					plan->rel_quals[i] = lappend(plan->rel_quals[i], qual);
				}
			}

			foreach(lc, relinfo[relno].comminfo) {
				_po_CommInfo *cinfo = (_po_CommInfo *) lfirst(lc);
				int other_relno;
				Oid myindexid;

				if (relno == cinfo->larg_varno) {
					myindexid = cinfo->larg_indexid;
					other_relno = cinfo->rarg_varno;
				}
				else {
					Assert(relno == cinfo->rarg_varno);
					myindexid = cinfo->rarg_indexid;
					other_relno = cinfo->larg_varno;
				}
				
				if (i > relinfo[other_relno].joinseq) {
					OpExpr *qual = (OpExpr *) copyObject(cinfo->qual);
					if (myindexid == indexid) {
						if (relno == cinfo->rarg_varno) {
							/* commute it */
							_po_swap_scalar(void *, linitial(qual->args), lsecond(qual->args));
							qual->opno = cinfo->commute_opno;
							qual->opfuncid = InvalidOid;
						}
						plan->join_quals[i] = lappend(plan->join_quals[i], qual);
					}
					else {
						plan->rel_quals[i] = lappend(plan->rel_quals[i], qual);
					}
				}
			}
		}
		
		/* add rel quals */
		{
			ListCell *lc;
			foreach (lc, rel_quals) {
				Node *qual = (Node *) copyObject(lfirst(lc));
				
				int relno = _po_find_relno_with_largest_joinseq(qual, relinfo);
				int joinseq;
				if (relno < 0) relno = 1;
				joinseq = relinfo[relno].joinseq;

				plan->rel_quals[joinseq] = lappend(plan->rel_quals[joinseq], qual);
			}
		}

		/* fix refs */
		for (i = 1; i <= nrel; ++i) {
			ListCell *lc;

			foreach(lc, plan->join_quals[i]) {
				OpExpr *qual = (OpExpr *) lfirst(lc);

				_po_fix_onlinesamplejoin_refs((Node *) qual, relinfo);
				_po_fix_opexpr_larg_varno_attno(qual, 0, 1);
				_po_fix_opexpr_funcid(qual);
			}

			foreach(lc, plan->rel_quals[i]) {
				Node *qual = (Node *) lfirst(lc);

				_po_fix_onlinesamplejoin_refs(qual, relinfo);
				if (IsA(qual, OpExpr)) {
					_po_fix_opexpr_funcid((OpExpr *) qual);
				}
			}
		}

		plan->plan.targetlist = (List *) copyObject(flat_tlist);
		_po_fix_onlinesamplejoin_refs(plan->plan.targetlist, relinfo);

		agg->candidate_join_plans = lappend(agg->candidate_join_plans, plan);
		DELETE_PATHINFO(cur_path);
	}

	/* clean up */
	{
		ListCell *lc;

		pfree(rtable_relid + 1);

		/* TODO deal with RelOptInfos ? */
		pfree(rtable_reloptinfo + 1);

		for (i = 1; i <= nrel; ++i) {
			list_free(relinfo[i].join_quals);
			list_free(relinfo[i].join_quals_indexid);
			foreach(lc, relinfo[i].comminfo) {
				_po_CommInfo *cinfo = (_po_CommInfo *) lfirst(lc);
				if (cinfo->larg_varno == i) {
					pfree(cinfo);
				}
			}
			list_free(relinfo[i].comminfo);
			bms_free(relinfo[i].outedges);
			bms_free(relinfo[i].inedges);
		}
		pfree(relinfo + 1);
		list_free(candidate_paths);
	}

#undef NEW_PATHINFO
#undef COPY_PATHINFO
#undef DELETE_PATHINFO
}

/**
 * Find the set of all relations referenced in the node.
 * The returned bitmap includes an additional element (n + 1), 
 * so the number of referenced relation in the bitmap is 
 * bms_num_members - 1. 
 *
 * TODO create a macro to find the number of referenced relations
 */
static Bitmapset *
_po_find_rel_set(Node *node, uint32 n) {
	_po_find_rel_set_context ctx;
	
	ctx.n = n;
	ctx.set = bms_make_singleton(n+1);

	_po_find_rel_set_impl(node, &ctx);
	return ctx.set;
}

static bool 
_po_find_rel_set_impl(Node *node, _po_find_rel_set_context *ctx) {
	
	if (IsA(node, Var)) {
		Var *var = (Var *) node;

		if (var->varno <= ctx->n) {
			bms_add_member(ctx->set, var->varno);
		}
		else {
			ereport(ERROR, (errmsg("unknown variable (%u, %u)", var->varno, var->varattno)));
		}
	}

	return expression_tree_walker(node, _po_find_rel_set_impl, ctx);
}

static int
_po_get_varno_if_is_var(Node *node) {
	if (IsA(node, Var)) {
		return ((Var *) node)->varno;	
	}

	return 0;
}

static Oid
_po_find_btindex_on_single_attr(RelOptInfo *reloptinfo, AttrNumber attrno) {
	ListCell	*lc;
	
	foreach(lc, reloptinfo->indexlist) {
		IndexOptInfo *indexoptinfo = (IndexOptInfo *) lfirst(lc);

		if (indexoptinfo->relam != BTREE_AM_OID) {
			continue;
		}
		if (indexoptinfo->ncolumns != 1) {
			continue;
		}

		if (indexoptinfo->indexkeys[0] == attrno) {
			return indexoptinfo->indexoid;
		}
	}

	return InvalidOid;
}

/*
static void
_po_sort_targetlist_and_fix_agg_refs(OnlineSampleJoin *plan, 
									 List *agginfo_list) {
	int ntargets = plan->ntargets;
	TargetEntry **target = plan->target;
	int *target_order = palloc(sizeof(int) * (ntargets + 1));
	int *inv_target_order = palloc(sizeof(int) * (ntargets + 1));
	int i;
	ListCell *lc;
	
	for (i = 1; i <= ntargets; ++i) target_order[i] = i;

	qsort_arg(target_order + 1, ntargets, sizeof(int), 
			  _po_sort_targetlist_cmpfunc, target);

	for (i = 1; i <= ntargets; ++i)
		inv_target_order[target_order[i]] = i;

	foreach(lc, agginfo_list) {
		Node *node = (Node *) lfirst(lc);

		if (IsA(node, OnlineAggSum)) {
			_po_fix_agginfo_expr_ref(((OnlineAggSum *) node)->arg, inv_target_order);
		}
		else if (IsA(node, OnlineAggCount)) {
			/ nothing to do /
		}
		else {
			ereport(ERROR, (errmsg("unrecognized node type %d", node->type)));
		}
	}

	for (i = 1; i <= ntargets; ++i) {
		target[i]->resno = target_order[i];
	}

	pfree(target_order);
	pfree(inv_target_order);
} */

/*
static int
_po_sort_targetlist_cmpfunc(const void *a, const void *b, void *arg) {
	TargetEntry **target = (TargetEntry **) arg;
	int id1 = *(int*) a;
	int id2 = *(int*) b;
	
	if (target[id1]->resorigtbl < target[id2]->resorigtbl) {
		return -1;
	}
	else if (target[id1]->resorigtbl == target[id2]->resorigtbl) {
		return 0;
	}
	return 1;
} */

/*
static void
_po_fix_agginfo_expr_ref(Expr *expr, int *inv_target_order) {
	_po_fix_agginfo_expr_ref_context ctx;

	ctx.inv_target_order = inv_target_order;

	_po_fix_agginfo_expr_ref_impl((Node *) expr, &ctx);
}

static bool
_po_fix_agginfo_expr_ref_impl(Node *node, _po_fix_agginfo_expr_ref_context *ctx) {
	
	if (IsA(node, Var)) {
		Var *var = (Var *) node;
		var->varattno = ctx->inv_target_order[var->varattno];
		return false;
	}

	return expression_tree_walker(node, _po_fix_agginfo_expr_ref_impl, ctx);
}
 */
static IndexOptInfo *_po_find_primary_index(List *index_opts) {
	ListCell *cl;
	IndexOptInfo *indexopt;
	HeapTuple indexTuple;
	bool isprimary;

	foreach(cl, index_opts) {
		indexopt = (IndexOptInfo *) lfirst(cl);
		
		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexopt->indexoid));	
		if (!HeapTupleIsValid(indexTuple)) {
			ereport(ERROR, (errmsg("cache lookup failed for index %u", indexopt->indexoid)));
		}
		isprimary = ((Form_pg_index) GETSTRUCT(indexTuple))->indisprimary;
		ReleaseSysCache(indexTuple);

		if (isprimary) 
			return indexopt;
	}
	
	ereport(ERROR, (errmsg("cannot find primary index")));
	return NULL;
}

static bool
_po_fix_onlinesamplejoin_refs_impl(Node *expr, _po_build_online_join_path_rel_info *relinfo) {

	if (IsA(expr, Var)) {
		Var *var = (Var *) expr;
		var->varnoold = var->varno;
		var->varno = relinfo[var->varno].joinseq;
		return false;
	}

	return expression_tree_walker(expr, _po_fix_onlinesamplejoin_refs_impl, (void *) relinfo);
}

static uint64
_po_get_count_from_btree(Oid indexid, List *quals) {
	Relation index;
	ScanKey scankeys;
	int n_scankeys;
	int i;
	ListCell *lc;
	BTSampleState samplestate;
	uint64 count;
	
	index = index_open(indexid, AccessShareLock);

	n_scankeys = list_length(quals);
	scankeys = (ScanKey) palloc(n_scankeys * sizeof(ScanKeyData));
	
	i = 0;
	foreach(lc, quals) {
		OpExpr *opexpr = (OpExpr *) lfirst(lc);
		Oid opno;
		RegProcedure opfuncid;
		Oid opfamily;
		int op_strategy;
		Oid op_lefttype;
		Oid op_righttype;
		Const *rightop = (Const *) lsecond(opexpr->args);
		AttrNumber attno = 1; 
		/* fix attno to 1 since we only operates on single attribute index, leftop->attno is not fixed at this point */
		Datum scanvalue;
		int flags = 0;

		_po_fix_opexpr_funcid(opexpr); /* funcid could be invalid at this moment */
		
		opno = opexpr->opno;
		opfuncid = opexpr->opfuncid;

		if (attno < 1 || attno > index->rd_index->indnatts) {
			elog(ERROR, "bogus index qualification");
		}

		opfamily = index->rd_opfamily[attno - 1];

		get_op_opfamily_properties(opno, opfamily, false, &op_strategy, &op_lefttype, &op_righttype);

		scanvalue = rightop->constvalue;
		if (rightop->constisnull)
			flags |= SK_ISNULL;

		ScanKeyEntryInitialize(&scankeys[i], flags, attno, op_strategy, op_righttype, opexpr->inputcollid,
				opfuncid, scanvalue);
		++i;
	}
	
	samplestate = _bt_prepare_sample_state(index, n_scankeys, scankeys, false);
	if (!_bt_count(samplestate)) {
		ereport(ERROR, (errmsg("error when counting using btree")));
	}
	count = _bt_sample_get_count(samplestate);

	_bt_destroy_sample_state(samplestate);
	pfree(scankeys);
	index_close(index, NoLock);

	return count;
}

static int 
_po_find_relno_with_largest_joinseq(Node *expr, 
		_po_build_online_join_path_rel_info *relinfo) {
	
	_po_find_relno_with_largest_joinseq_context ctx;
	
	ctx.relinfo = relinfo;
	ctx.res_relno = -1;

	_po_find_relno_with_largest_joinseq_impl(expr, &ctx);
	return ctx.res_relno;
}

static bool 
_po_find_relno_with_largest_joinseq_impl(Node *expr, 
		_po_find_relno_with_largest_joinseq_context *ctx) {
	
	if (IsA(expr, Var)) {
		Var *var = (Var *) expr;
		if (ctx->res_relno == -1 || 
			ctx->relinfo[var->varno].joinseq > ctx->relinfo[ctx->res_relno].joinseq) {
			ctx->res_relno = var->varno;
		}
		return false;
	}

	return expression_tree_walker(expr, _po_find_relno_with_largest_joinseq_impl, (void *) ctx);
}


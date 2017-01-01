/*----------------------------------------------------------
 *
 * nodeOnline.c
 *  Routines to handle online aggregation queries
 *
 * Copyright (c) 2015-2017, InitialD Lab
 *
 * IDENTIFICATION
 *    src/backend/executor/nodeOnline.c
 *
 *----------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "utils/onlineagg_numeric.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
#include "access/nbtree.h"
#include "utils/xql_math.h"
#include "utils/avltree.h"

typedef struct OnlineAggHashEntryData *OnlineAggHashEntry;

typedef struct OnlineAggHashEntryData {
	TupleHashEntryData shared;
	
	uint64 nsampled;
	OnlineAgg_NumericAggPerGroupState *pergroups[1];
} OnlineAggHashEntryData;


static void onlineagg_init_pergroups(
		OnlineAggState *state,
		OnlineAgg_NumericAggPerGroupState **pergroups);
static void init_online_sample_join(OnlineSampleJoin *node, EState *estate, int eflags,
									OnlineSampleJoinState *state);
static void init_adaptive_online_sample_join(OnlineSampleJoin *node, 
											 EState *estate, int eflags,
											AdaptiveOnlineSampleJoinState *state);
static OnlineSampleJoinState *online_reinit_join(
		OnlineSampleJoinState *state, OnlineSampleJoin *node);
static Datum online_relqual_scalarVarEval(ExprState *expression, ExprContext *econtext,
										  bool *isNull, ExprDoneCond *isDone);
static Datum online_relqual_scalarVarEval_fast(ExprState *exprstate, 
			ExprContext *econtext, bool *isNull, ExprDoneCond *isDone);
static void online_ExecBuildScanKeys(PlanState *planstate, Relation index,
							  List *quals, 
							  ScanKey *scanKeys, int *numScanKeys,
							  IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys);
static void onlineagg_initialize_resulttupleslot(OnlineAgg *node, 
												EState *estate, 
												OnlineAggState *planstate);
static TupleTableSlot *onlineagg_exec_grouping(OnlineAggState *state);
static bool onlineagg_fill_hash_table(OnlineAggState *state);
static OnlineAggHashEntry online_sample_lookup_hash_entry(OnlineAggState *state, 
											TupleTableSlot *keyslot);
static TupleTableSlot *onlineagg_hash_table_retrieve_next(OnlineAggState *state);
static TupleTableSlot *onlineagg_exec_no_grouping(OnlineAggState *state);
static void onlineagg_output_pergroups(OnlineAggState *state, 
						   TupleTableSlot *firsttup,
						   OnlineAgg_NumericAggPerGroupState **pergroups,
						   uint64 nsampled);
static void onlineagg_output_separator(OnlineAggState *state);
static void onlineagg_advance_pergroups(OnlineAggState *state, 
							TupleTableSlot *slot,
							OnlineAgg_NumericAggPerGroupState **pergroups,
							uint64 *nsampled);
static TupleTableSlot *onlineagg_exec_planselection(OnlineAggState *state);
static void onlineagg_output_planselection(OnlineAggState *state,
							   OnlineSampleJoin *join_plan,
							   uint64 nrejected,
							   int64 time_elapsed);
static void online_sample_join_set_outputs(OnlineSampleJoinState *state);
static TupleTableSlot *online_sample_from_btree(OnlineSampleJoinState *state, int i);
static void online_eval_runtime_keys(ExprContext *econtext, 
							IndexRuntimeKeyInfo *runtime_keys,
							int n_runtime_keys);
static HeapTuple online_fetch_heap(OnlineSampleJoinState *state, BTSampleState sstate, int i);

static int online_avltree_cmp(TupleTableSlot *slot1, TupleTableSlot *slot2, void *cxt);

static int64 online_timestamp_diff_millisecond(TimestampTz start_time, TimestampTz end_time) {
	long secs;
	int microsecs;

	TimestampDifference(start_time, end_time, &secs, &microsecs);
	return ((int64) secs) * 1000ll + ((int64) microsecs) / 1000ll;
}

OnlineAggState *
ExecInitOnlineAgg(OnlineAgg *node, EState *estate, int eflags) {
	OnlineAggState *aggstate;
	ListCell *lc;

	aggstate = makeNode(OnlineAggState);
	aggstate->ps.state = estate;
	aggstate->ps.plan = (Plan *) node;

	aggstate->aggcontext = AllocSetContextCreate(CurrentMemoryContext, "OnlineAggContext",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);
	
	aggstate->nInitSamples = estate->es_plannedstmt->nInitSamples;
	if (aggstate->nInitSamples > 0) {
		OnlineSampleJoin *join_plan = linitial(node->candidate_join_plans);
		aggstate->next_plan_cell = list_head(node->candidate_join_plans);
		aggstate->best_plan = NULL;
		aggstate->nrejected_best_plan = aggstate->nInitSamples;
		aggstate->join_state = ExecInitOnlineSampleJoin(join_plan, estate, eflags);
		aggstate->join_state->forOutputs = false;
	}
	else {
		OnlineSampleJoin *join_plan = linitial(node->candidate_join_plans);
		aggstate->next_plan_cell = NULL;
		aggstate->join_state = ExecInitOnlineSampleJoin(join_plan, estate, eflags);
		aggstate->join_state->forOutputs = true;
	}
	
	
	/* create expression context */
	aggstate->expr_ctx = CreateExprContext(estate);
	
	aggstate->timeout_time = 0;
	aggstate->time_elapsed = 0;
	
	/* init. peragg states and non agg cols */
	aggstate->naggs = 0;
	aggstate->peraggs = NIL;
	aggstate->target_states= NIL;
	foreach (lc, node->plan.targetlist) {
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr = tle->expr;

		OnlineAgg_NumericAggPerAggState *peragg =
			init_OnlineAgg_NumericAggPerAggState(expr);
		
		/* agg */
		if (NULL != peragg) {
			if (NULL != peragg->expr) {
				peragg->expr_state = ExecInitExpr(peragg->expr, (PlanState* ) aggstate);
			}
			
			aggstate->peraggs = lappend(aggstate->peraggs, peragg);
			aggstate->target_states = lappend(aggstate->target_states, peragg);
			++aggstate->naggs;
		}

		/* non-agg */
		else {
			ExprState *expr_state = ExecInitExpr(expr, (PlanState *) aggstate);
			aggstate->target_states = lappend(aggstate->target_states, 
											  expr_state);
		}
	}
	
	/* has grouping, init. hash table */
	if (node->numGrpCols > 0) {
		Size entrysize;
		
		aggstate->hasGrouping = true;
		/* lookups eq and hash functions */
		execTuplesHashPrepare(node->numGrpCols,
							  node->grpEqOps,
							  &aggstate->eqfunctions,
							  &aggstate->hashfunctions);
		
		/* build hash table */
		entrysize = sizeof(OnlineAggHashEntryData) + 
			(aggstate->naggs - 1) * sizeof(OnlineAgg_NumericAggPerGroupState*);
		aggstate->hashtable = BuildTupleHashTable(
								node->numGrpCols,
								node->grpColsIdx,
								aggstate->eqfunctions,
								aggstate->hashfunctions,
								0x7FFFFFFF, /* initialize as many buckets as possible*/
								entrysize,
								aggstate->aggcontext,
								aggstate->expr_ctx->ecxt_per_tuple_memory);
		
		/* create hash slot and set up the tupledesc */
		aggstate->hashslot = ExecInitExtraTupleSlot(estate);
		ExecSetSlotDescriptor(aggstate->hashslot, 
				aggstate->join_state->outputs->tts_tupleDescriptor);
		ExecStoreAllNullTuple(aggstate->hashslot);
	
		aggstate->table_filled = false;
	}
	/* no grouping */
	else {
		/* init pergroups */
		aggstate->nsampled = 0;
		aggstate->hasGrouping = false;
		aggstate->pergroups = (OnlineAgg_NumericAggPerGroupState **) 
			palloc0(sizeof(OnlineAgg_NumericAggPerGroupState *) * aggstate->naggs);

		onlineagg_init_pergroups(aggstate, aggstate->pergroups);
	}
	aggstate->ntotal_sampled = 0;
	
	onlineagg_initialize_resulttupleslot(node, estate, aggstate);

	return aggstate;
}

static void onlineagg_init_pergroups(
		OnlineAggState *state,
		OnlineAgg_NumericAggPerGroupState **pergroups) {
	
	ListCell *lc;
	List *peraggs = state->peraggs;
	int i = 0;

	foreach(lc, peraggs) {
		OnlineAgg_NumericAggPerAggState *peragg = 
			(OnlineAgg_NumericAggPerAggState *) lfirst(lc);

		pergroups[i++] = peragg->initfunc(state->aggcontext, peragg);
	}
}

static void
init_online_sample_join(OnlineSampleJoin *node, EState *estate, int eflags,
						OnlineSampleJoinState *state) {
	uint32	i;
	MemoryContext oldcontext;

	if (!state->initialized) {
		state->ps.plan = (Plan *) node;
		state->ps.state = estate;
		
		state->nrels = node->nrel;
		state->tuples = (TupleTableSlot **) palloc(sizeof(TupleTableSlot*) * (state->nrels + 1));
		state->heaprels = (Relation *) palloc(sizeof(Relation) * (state->nrels + 1));
		state->join_index = (Relation *) palloc(sizeof(Relation) * (state->nrels + 1));
		state->n_joinkeys = (int *) palloc(sizeof(int) * (state->nrels + 1));
		state->join_keys = (ScanKey *) palloc(sizeof(ScanKey) * (state->nrels + 1));
		state->rel_quals = (List **) palloc(sizeof(List *) * (state->nrels + 1));
		state->runtime_keys = (IndexRuntimeKeyInfo **)
								palloc(sizeof(IndexRuntimeKeyInfo *) * (state->nrels + 1));
		state->n_runtime_keys = (int *) palloc(sizeof(int) * (state->nrels + 1));
		
		state->rel_qual_ctx = CreateExprContext(estate);
		state->rel_qual_ctx->ecxt_reltuples = state->tuples;
		state->sample_state = (BTSampleState *) palloc0(sizeof(BTSampleState) * (state->nrels + 1));
		state->inv_prob = (uint64 *) palloc(sizeof(uint64) * (state->nrels + 1));

		state->buf = (Buffer *) palloc(sizeof(Buffer) * (state->nrels + 1));
		state->heap_tup = (HeapTupleData *) palloc(sizeof(HeapTupleData) * (state->nrels + 2));
		
		state->tuples[0] = NULL;
		state->heaprels[0] = InvalidRelation;
		state->buf[0] = InvalidBuffer;
		state->join_index[0] = InvalidRelation;

		state->n_joinkeys[0] = 0;
		state->join_keys[0] = NULL;
		state->rel_quals[0] = NIL;
		state->runtime_keys[0] = NULL;
		state->n_runtime_keys[0] = 0;
		for (i = 1; i <= state->nrels; ++i) {
			state->tuples[i] = ExecInitExtraTupleSlot(estate);
			state->heaprels[i] = InvalidRelation;
			state->buf[i] = InvalidBuffer;
			state->join_index[i] = InvalidRelation;
		}

		state->outputs = ExecInitExtraTupleSlot(estate);

		state->perPlanContext = AllocSetContextCreate(CurrentMemoryContext, 
												"OnlineSampleJoinState",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

		state->initialized = true;
	}
	else {
		for (i = 1; i <= state->nrels; ++i) {
			ExecClearTuple(state->tuples[i]);
			if (BufferIsValid(state->buf[i]))
				ReleaseBuffer(state->buf[i]);
			
			Assert(RelationIsValid(state->heaprels[i]));
			Assert(RelationIsValid(state->join_index[i]));
			heap_close(state->heaprels[i], NoLock);
			index_close(state->join_index[i], NoLock);

			state->sample_state[i] = NULL;
		}
		ExecClearTuple(state->outputs);
		MemoryContextReset(state->perPlanContext);
	}

	
	for (i = 1; i <= state->nrels; ++i) {
		Relation heap;
		TupleDesc tupdesc;
		Relation index = InvalidRelation;
		List *join_quals;
		Oid	indexid;
		
		/* init heaps */
		heap = heap_open(node->relid[i], AccessShareLock);
		tupdesc = RelationGetDescr(heap);

		state->heaprels[i] = heap;
		ExecSetSlotDescriptor(state->tuples[i], tupdesc);

		state->buf[i] = InvalidBuffer;
		
		/* init indecies */
		join_quals = node->join_quals[i];
		if (join_quals == NIL) {
			/* use the primary index to perform sample */
			indexid = node->primary_indexid[i];
		}
		else {
			indexid = node->indexid[i];
		}

		Assert(OidIsValid(indexid));
		index = index_open(indexid, AccessShareLock);
		state->join_index[i] = index;
	}
	
	/* init. outputs */
	{
		List*	tlist = node->plan.targetlist;
		TupleDesc outputdesc;

		state->ntargets = list_length(tlist);
		/* init. outputdesc */
		outputdesc = ExecTypeFromTL(tlist, false);	
		ExecSetSlotDescriptor(state->outputs, outputdesc);
	}

	oldcontext = MemoryContextSwitchTo(state->perPlanContext);

	/* init quals (in perPlanContext)  */
	for (i = 1; i <= state->nrels; ++i) {
		List *rel_quals = NIL;
		ListCell *lc;
		ExprState *expr_state;

		/* init join quals */
		state->n_runtime_keys[i] = 0;
		online_ExecBuildScanKeys((PlanState  *) state, 
								 state->join_index[i],
								 node->join_quals[i], 
								 &state->join_keys[i], &state->n_joinkeys[i],
								 &state->runtime_keys[i], &state->n_runtime_keys[i]);
		
		/* initialize rel quals */
		foreach(lc, node->rel_quals[i]) {
			Expr *qual = (Expr *) lfirst(lc);

			expr_state = online_ExecInitExpr(qual, (PlanState *) state,
											 online_relqual_scalarVarEval);
			rel_quals = lappend(rel_quals, expr_state);
		}
		state->rel_quals[i] = rel_quals;
	}

	/* initialize output expr states (in perPlanContext) */
	{
		List*	tlist = node->plan.targetlist;
		ListCell *lc;

		state->output_expr_states = NIL;
		foreach(lc, tlist) {
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			ExprState *expr_state = 
				online_ExecInitExpr(tle->expr, (PlanState *) state,
									online_relqual_scalarVarEval);
			state->output_expr_states = lappend(state->output_expr_states, expr_state);
		}
	}
	MemoryContextSwitchTo(oldcontext);
}

static void
init_adaptive_online_sample_join(OnlineSampleJoin *node, EState *estate, int eflags,
								 AdaptiveOnlineSampleJoinState *state) {
	
	uint32	nrels = node->nrel, 
			i;
	MemoryContext oldcontext;
	OnlineSampleJoinState *ostate = (OnlineSampleJoinState *) state;
	
	if (!state->initialized) {
		state->ainfo = (OnlineAgg_adaptive_info_t *) 
					palloc(sizeof(OnlineAgg_adaptive_info_t) * (nrels + 1));
		
		/* find bt compare routines */
		state->pk_ncolumns = (int *) palloc(sizeof(int) * (nrels + 1));
		state->pk_attno = (int **) palloc(sizeof(int **) * (nrels + 1));
		state->pk_collation = (Oid **) palloc(sizeof(Oid *) * (nrels + 1));
		state->pk_fmgrinfo = (FmgrInfo **) palloc(sizeof(FmgrInfo *) * (nrels + 1));

		state->pk_ncolumns[0] = 0;
		state->pk_attno[0] = NULL;
		state->pk_collation[0] = NULL;
		state->pk_fmgrinfo[0] = NULL;

		/* set up wtree context */
		state->wtree_context = AllocSetContextCreate(CurrentMemoryContext, 
													"AdaptiveOnlineAggWTreeContext",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);
		state->join_path = (avlnode **) palloc(sizeof(avlnode *) * (nrels + 1));
		state->initialized = true;	
	}
	
	oldcontext = MemoryContextSwitchTo(ostate->perPlanContext);

	for (i = 1; i <= nrels; ++i) {
		IndexOptInfo *iinfo = node->primaryindex_info[i];
		Oid opfuncid;
		int ncolumns, j;
		
		ncolumns = iinfo->ncolumns;
		state->pk_ncolumns[i] = ncolumns;
		state->pk_attno[i] = iinfo->indexkeys;
		state->pk_collation[i] = iinfo->indexcollations;
		

		state->pk_fmgrinfo[i] = (FmgrInfo *) palloc(sizeof(FmgrInfo) * ncolumns);
		for (j = 0; j < ncolumns; ++j) {
			opfuncid = get_opfamily_proc(iinfo->opfamily[j],
										 iinfo->opcintype[j],
										 iinfo->opcintype[j],
										 BTORDER_PROC); 
			fmgr_info(opfuncid, &state->pk_fmgrinfo[i][j]);
		}
	}

	MemoryContextSwitchTo(oldcontext);
		
	MemoryContextReset(state->wtree_context);
	state->wtree_root = NULL;
	state->slots = NIL;	
}

OnlineSampleJoinState *
ExecInitOnlineSampleJoin(OnlineSampleJoin *node, EState *estate, int eflags) {
	if (node->adaptive) {
		AdaptiveOnlineSampleJoinState *astate;
		OnlineSampleJoinState *state;

		astate = makeNode(AdaptiveOnlineSampleJoinState);
		state = (OnlineSampleJoinState *) astate;
		state->initialized = false;
		astate->initialized = false;

		init_online_sample_join(node, estate, eflags, (OnlineSampleJoinState *) astate);
		
		init_adaptive_online_sample_join(node, estate, eflags, astate);
		return (OnlineSampleJoinState *) astate;
	}
	else {
		OnlineSampleJoinState *state;

		state = makeNode(OnlineSampleJoinState);
		state->initialized = false;

		init_online_sample_join(node, estate, eflags, state);
		return state;
	}
}

static OnlineSampleJoinState *
online_reinit_join(OnlineSampleJoinState *state, OnlineSampleJoin *node) {
	if (IsA(state, OnlineSampleJoinState)) {
		Assert(!node->adaptive);

		init_online_sample_join(node, state->ps.state, 0, state);
	}
	else {
		Assert(node->adaptive);

		init_online_sample_join(node, state->ps.state, 0, state);
		init_adaptive_online_sample_join(node, state->ps.state, 0, 
				(AdaptiveOnlineSampleJoinState *) state);
	}
	
	return state;
}

/* evalulate to varattno-th element of econtext->ecxt_reltuples[varno] */
static Datum online_relqual_scalarVarEval(ExprState *expression, ExprContext *econtext,
								   bool *isNull, ExprDoneCond *isDone) {
	Var	*var = (Var *) expression->expr;
	AttrNumber attnum = var->varattno;
	TupleTableSlot *slot = econtext->ecxt_reltuples[var->varno];

	Assert(attnum != InvalidAttrNumber);

	if (isDone)
		*isDone = ExprSingleResult;

	if (NULL == slot) {
		*isNull = true;
		return PointerGetDatum(NULL);
	}

	if (attnum > 0) {
		TupleDesc slot_tupdesc = slot->tts_tupleDescriptor;
		Form_pg_attribute attr;

		if (attnum > slot_tupdesc->natts) {
			elog(ERROR, "attribute number %d exceeds number of columns %d",
				 attnum, slot_tupdesc->natts);
		}

		attr = slot_tupdesc->attrs[attnum - 1];

		if (!attr->attisdropped) {
			if (var->vartype != attr->atttypid) {
				ereport(ERROR,
						(errmsg("attribute %d has wrong type", attnum),
						 errdetail("Table has type %s, but query expects %s.",
								   format_type_be(attr->atttypid),
								   format_type_be(var->vartype))));
			}
		}
	}

	expression->evalfunc = online_relqual_scalarVarEval_fast;

	return slot_getattr(slot, attnum, isNull);
}

static Datum online_relqual_scalarVarEval_fast(ExprState *exprstate, 
										ExprContext *econtext,
										bool *isNull, ExprDoneCond *isDone) {
	TupleTableSlot *slot = econtext->ecxt_reltuples[((Var *) exprstate->expr)->varno];

	if (isDone)
		*isDone = ExprSingleResult;

	if (NULL == slot) {
		*isNull = true;
		return PointerGetDatum(NULL);
	}

	return slot_getattr(slot, ((Var *) exprstate->expr)->varattno, isNull);
}

static void 
online_ExecBuildScanKeys(PlanState *planstate, Relation index,
							  List *quals, 
							  ScanKey *scanKeys, int *numScanKeys,
							  IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys) {

	ListCell				*qual_cell;
	ScanKey					scan_keys;
	IndexRuntimeKeyInfo		*runtime_keys;
	int						n_scan_keys;
	int						n_runtime_keys;
	int						max_runtime_keys;
	int						i;

	n_scan_keys = list_length(quals);
	scan_keys = (ScanKey) palloc(n_scan_keys * sizeof(ScanKeyData));

	runtime_keys = *runtimeKeys;
	n_runtime_keys = max_runtime_keys = *numRuntimeKeys;

	i = 0;
	foreach(qual_cell, quals) {
		Expr		*clause = (Expr *) lfirst(qual_cell);
		ScanKey		cur_scankey = &scan_keys[i++];
		Oid			opno;
		RegProcedure opfuncid;
		Oid			opfamily;
		int			op_strategy;
		Oid			op_lefttype;
		Oid			op_righttype;
		Expr		*leftop;
		Expr		*rightop;
		AttrNumber	varattno;

		if (IsA(clause, OpExpr)) {
			int flags = 0;
			Datum scanvalue;

			opno = ((OpExpr *) clause)->opno;
			opfuncid = ((OpExpr *) clause)->opfuncid;

			leftop = (Expr *) get_leftop(clause);
			
			/* RelabelType not supported */

			Assert(leftop != NULL);

			if (!IsA(leftop, Var))
				elog(ERROR, "indexqual does not have key on the left hand side");

			varattno = ((Var *) leftop)->varattno;
			if (varattno < 1 || varattno > index->rd_index->indnatts)
				elog(ERROR, "bogus index qualification");

			opfamily = index->rd_opfamily[varattno - 1];

			get_op_opfamily_properties(opno, opfamily, false,
									   &op_strategy,
									   &op_lefttype,
									   &op_righttype);

			rightop = (Expr *) get_rightop(clause);

			/* RelabelType not supported */

			Assert(rightop != NULL);

			if (IsA(rightop, Const)) {
				scanvalue = ((Const *) rightop)->constvalue;
				if (((Const *) rightop)->constisnull)
					flags |= SK_ISNULL;
			}
			else {
				/* runtime key */
				if (max_runtime_keys == 0) {
					max_runtime_keys = 8;
					runtime_keys = (IndexRuntimeKeyInfo *)
						palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
				}
				else {
					max_runtime_keys <<= 1;
					runtime_keys = (IndexRuntimeKeyInfo *)
						repalloc(runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
				}
				runtime_keys[n_runtime_keys].scan_key = cur_scankey;
				runtime_keys[n_runtime_keys].key_expr = 
					online_ExecInitExpr(rightop, planstate, online_relqual_scalarVarEval);
				runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
				++n_runtime_keys;
				scanvalue = (Datum) NULL;
			}

			ScanKeyEntryInitialize(cur_scankey,
								   flags,
								   varattno,
								   op_strategy,
								   op_righttype,
								   ((OpExpr *) clause)->inputcollid,
								   opfuncid,
								   scanvalue);
		}
		/* TODO support row compare */
		/*else if (IsA(clause, RowCompareExpr)) */
		else {
			elog(ERROR, "unsupported index qual type: %d", (int) nodeTag(clause));
		}
	}

	Assert(n_runtime_keys <= max_runtime_keys);
	
	*scanKeys = scan_keys;
	*numScanKeys = n_scan_keys;
	*runtimeKeys = runtime_keys;
	*numRuntimeKeys = n_runtime_keys;
}

static void
onlineagg_initialize_resulttupleslot(
		OnlineAgg *node, EState *estate, OnlineAggState *aggstate) {
	PlanState * planstate = (PlanState *) aggstate;
	TupleTableSlot *slot;
	TupleDesc tupdesc;
	int i;
	List *tlist = node->plan.targetlist;
	ListCell *lc;
	int ntargets = list_length(tlist);
	int withPlanSelection = (aggstate->nInitSamples > 0) ? 1 : 0;
	int ncols = withPlanSelection + 3 + aggstate->naggs + ntargets;
	
	ExecInitResultTupleSlot(estate, planstate);
	slot = planstate->ps_ResultTupleSlot;

	tupdesc = CreateTemplateTupleDesc(ncols, false);
	
	i = 1;
	/* column plan no. (int32) if nInitSamples > 0 */
	if (withPlanSelection)
		TupleDescInitEntry(tupdesc, i++, "plan no.", INT4OID, 0, 0);

	/* column time elapsed in ms (int64) */
	TupleDescInitEntry(tupdesc, i++, "time (ms)", INT8OID, 0, 0);

	/* column number of samples taken (int64) */
	TupleDescInitEntry(tupdesc, i++, "nsamples", INT8OID, 0, 0);
    
    /* column number of rejected samples (int64) */
    TupleDescInitEntry(tupdesc, i++, "nrejected", INT8OID, 0, 0);

	foreach(lc, tlist) {
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		/* column i result (numeric) */
		/* column i + 1 CI / result*/
		if (IsA(tle->expr, OnlineAggSum)) {
			TupleDescInitEntry(tupdesc, i++, "sum", NUMERICOID, 0, 0);
			TupleDescInitEntry(tupdesc, i++, "rel. CI", NUMERICOID, 0, 0);
		}
		else if (IsA(tle->expr, OnlineAggCount)) {
			TupleDescInitEntry(tupdesc, i++, "count", NUMERICOID, 0, 0);
			TupleDescInitEntry(tupdesc, i++, "rel. CI", NUMERICOID, 0, 0);
		}

		/* column i expr of grouping cols */
		else {
			TupleDescInitEntry(tupdesc, 
							   i, 
							   tle->resname, 
							   exprType((Node *) tle->expr),
							   exprTypmod((Node *) tle->expr),
							   0);
			TupleDescInitEntryCollation(tupdesc,
										i++,
										exprCollation((Node *) tle->expr));
								
		}
	}

	ExecSetSlotDescriptor(slot, tupdesc);
}

TupleTableSlot *
ExecOnlineAgg(OnlineAggState *state) {
	if (NULL != state->next_plan_cell) {
		return onlineagg_exec_planselection(state);	
	}

	if (state->hasGrouping) {
		return onlineagg_exec_grouping(state);
	}
	else {
		return onlineagg_exec_no_grouping(state);
	}
}

static TupleTableSlot *
onlineagg_exec_grouping(OnlineAggState *state) {
	if (!state->table_filled) {
		if (!onlineagg_fill_hash_table(state)) {
			return NULL;
		}
	}

	return onlineagg_hash_table_retrieve_next(state);
}

static bool
onlineagg_fill_hash_table(OnlineAggState *state) {
	PlannedStmt *plan = state->ps.state->es_plannedstmt;
	TimestampTz start_time, now_time, end_time;
	
	if (state->timeout_time >= plan->withTime) 
		return false;

	start_time = GetCurrentTimestamp();

	state->timeout_time += plan->reportInterval;
	if (plan->withTime < state->timeout_time) state->timeout_time = plan->withTime;
	end_time = TimestampTzPlusMilliseconds(start_time, state->timeout_time - state->time_elapsed);	

	do {
		TupleTableSlot *slot;
		OnlineAggHashEntry entry;

		slot = ExecProcNode((PlanState *) state->join_state);
		
		if (slot == NULL) {
			++state->ntotal_sampled;
		}
		else {
			entry = online_sample_lookup_hash_entry(state, slot);
		
			onlineagg_advance_pergroups(state, slot, 
										entry->pergroups,
										&entry->nsampled);
		}
	} while ((now_time = GetCurrentTimestamp()) < end_time);

	state->time_elapsed += online_timestamp_diff_millisecond(start_time, now_time);
	
	/* initialize an iterator (can't freeze the hash table since we'll 
	 * continue updating after all tuples are retrieved) */
	InitTupleHashIterator(state->hashtable, &state->hashiter);
	return (state->table_filled = true);
}


static OnlineAggHashEntry
online_sample_lookup_hash_entry(OnlineAggState *state, TupleTableSlot *keyslot) {
	TupleTableSlot *hashslot = state->hashslot;
	OnlineAgg *agg = (OnlineAgg *) state->ps.plan;
	OnlineAggHashEntry entry;
	int i;
	bool isnew;
	
	Assert(agg->numGrpCols > 0);
	slot_getsomeattrs(keyslot, agg->grpColsIdx[0]);
	for (i = 0; i < agg->numGrpCols; ++i) {
		AttrNumber varno = agg->grpColsIdx[i] - 1;

		hashslot->tts_values[varno] = keyslot->tts_values[varno];
		hashslot->tts_isnull[varno] = keyslot->tts_isnull[varno];
	}

	entry = (OnlineAggHashEntry) LookupTupleHashEntry(state->hashtable,
				 									  hashslot,
													  &isnew);

	if (isnew) {
		/* init. agg for the new entry */
		entry->nsampled = 0;
		onlineagg_init_pergroups(state, entry->pergroups);
	}

	return entry;
}

static TupleTableSlot *
onlineagg_hash_table_retrieve_next(OnlineAggState *state) {
	OnlineAggHashEntry entry;
	TupleTableSlot *outputs = state->join_state->outputs;

	entry = (OnlineAggHashEntry) ScanTupleHashTable(&state->hashiter);
	if (entry == NULL) {
		/* no need to explicitly call TermTupleHashTable */
		state->table_filled = false;
		onlineagg_output_separator(state);
	}
	else {
		ExecStoreMinimalTuple(entry->shared.firstTuple, outputs, false);
		onlineagg_output_pergroups(state, outputs, entry->pergroups, entry->nsampled);
	}

	return state->ps.ps_ResultTupleSlot;
}

static TupleTableSlot *
onlineagg_exec_no_grouping(OnlineAggState *state) {
	PlannedStmt *plan = state->ps.state->es_plannedstmt;
	TimestampTz start_time, now_time, end_time;

	if (state->timeout_time >= plan->withTime)
		return false;

	start_time = GetCurrentTimestamp();

	state->timeout_time += plan->reportInterval;
	if (plan->withTime < state->timeout_time) state->timeout_time = plan->withTime;
	end_time = TimestampTzPlusMilliseconds(start_time, state->timeout_time - state->time_elapsed);

	/* run sampling until timeout */
	do {
		TupleTableSlot *slot;

		slot = ExecProcNode((PlanState *) state->join_state);

		onlineagg_advance_pergroups(state, slot, 
									state->pergroups,
									&state->nsampled);
	} while ((now_time = GetCurrentTimestamp()) < end_time);

	state->time_elapsed += online_timestamp_diff_millisecond(start_time, now_time);
	
	/* output */
	onlineagg_output_pergroups(state, NULL, state->pergroups, state->nsampled);

	return state->ps.ps_ResultTupleSlot;
}

static void
onlineagg_output_pergroups(OnlineAggState *state, 
						   TupleTableSlot *firsttup,
						   OnlineAgg_NumericAggPerGroupState **pergroups,
						   uint64 nsampled) {
	PlannedStmt *plan = state->ps.state->es_plannedstmt;
	TupleTableSlot *slot = state->ps.ps_ResultTupleSlot;
	ExprContext *econtext = state->expr_ctx;
	MemoryContext oldcontext;
	List *target_states = state->target_states;
	ListCell *lc;
	int i, j;
	Datum value;
	Datum rel_ci;
	bool isNull;

	/* the final agg functions and exprs are evaluated in the per tuple context
	 * so that the temporaries are reset in the next invocation of ExecOnlineAgg */
	ResetExprContext(econtext);
	econtext->ecxt_outertuple = firsttup;
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	ExecClearTuple(slot);
		
	i = 0;
	/* column plan no. if nInitSamples > 0 */
	if (state->nInitSamples > 0) {
		slot->tts_values[i] = Int32GetDatum(state->best_plan->no);
		slot->tts_isnull[i++] = false;
	}

	/* column time elapsed */	
	slot->tts_values[i] = Int64GetDatumFast(state->time_elapsed);
	slot->tts_isnull[i++] = false;

	/* column nsamples */
	slot->tts_values[i] = Int64GetDatumFast(nsampled);
	slot->tts_isnull[i++] = false;

    /* column nrejected */
    slot->tts_values[i] = Int64GetDatumFast(state->ntotal_sampled - nsampled);
    slot->tts_isnull[i++] = false;

	j = 0;
	foreach(lc, target_states) {
		Node *node = (Node *) lfirst(lc);	
		
		/* agg col */
		if (IsA(node, OnlineAgg_NumericAggPerAggState)) {
			OnlineAgg_NumericAggPerAggState *peragg = 
				(OnlineAgg_NumericAggPerAggState *) node;

			peragg->finalfunc(pergroups[j], peragg,
								plan->confidence,
								state->ntotal_sampled,
								&value,
								&rel_ci);

			slot->tts_values[i] = value;
			slot->tts_values[i+1] = rel_ci;
			slot->tts_isnull[i] = slot->tts_isnull[i+1] = false;
			
			i += 2;
			++j;
		}
		
		/* non-agg col */
		else {
			ExprState *expr_state = (ExprState *) node;

			value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);	

			slot->tts_values[i] = value;
			slot->tts_isnull[i] = isNull;

			++i;
		}
	}

	ExecStoreVirtualTuple(slot);
	MemoryContextSwitchTo(oldcontext);
}

static void
onlineagg_output_separator(OnlineAggState *state) {
	TupleTableSlot *slot = state->ps.ps_ResultTupleSlot;
	int natts = slot->tts_tupleDescriptor->natts;
	int i;
	
	ExecClearTuple(slot);
	for (i = 0; i < natts; ++i) {
		slot->tts_values[i] = PointerGetDatum(NULL);
		slot->tts_isnull[i] = true;
	}
	ExecStoreVirtualTuple(slot);
}

static void
onlineagg_advance_pergroups(OnlineAggState *state, 
							TupleTableSlot *slot,
							OnlineAgg_NumericAggPerGroupState **pergroups,
							uint64 *nsampled) {
	
	int i;
	List *peraggs = state->peraggs;
	ListCell *lc;
	ExprContext *econtext = state->expr_ctx;;

	
	++state->ntotal_sampled;
	if (IsA(state->join_state, OnlineSampleJoinState)) {
		if (NULL == slot) {
			i = 0;
			foreach(lc, peraggs) {
				OnlineAgg_NumericAggPerAggState *peragg = 
					(OnlineAgg_NumericAggPerAggState *) lfirst(lc);
				
				peragg->transfunc(pergroups[i], peragg, 
						PointerGetDatum(NULL), true, NULL, 0);
				++i;
			}
		}

		else {
			OnlineSampleJoinState *join_state = state->join_state;
			MemoryContext oldcontext;

			ResetExprContext(econtext);
			econtext->ecxt_outertuple = slot;

			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
			
			i = 0;
			foreach(lc, peraggs) {
				OnlineAgg_NumericAggPerAggState *peragg = 
					(OnlineAgg_NumericAggPerAggState *) lfirst(lc);
				
				Datum value;
				bool isNull;
					
				if (peragg->expr_state != NULL) {
					value = ExecEvalExpr(peragg->expr_state, econtext, &isNull, NULL);
				}
				else {
					value = PointerGetDatum(NULL);	/* to suppress compiler warning */
					isNull = false;
				}

				peragg->transfunc(pergroups[i], peragg,
						value, isNull, join_state->inv_prob, join_state->nrels);

				++i;
			}

			MemoryContextSwitchTo(oldcontext);
			++*nsampled;
		}
	}

	else if (IsA(state->join_state, AdaptiveOnlineSampleJoinState)) {
		if (NULL == slot) {
			i = 0;
			foreach(lc, peraggs) {
				OnlineAgg_NumericAggPerAggState *peragg = 
					(OnlineAgg_NumericAggPerAggState *) lfirst(lc);
				
				peragg->a_transfunc(pergroups[i], peragg, 
						PointerGetDatum(NULL), true, NULL, 0);
				++i;
			}
		}

		else {
			AdaptiveOnlineSampleJoinState *join_state = 
				(AdaptiveOnlineSampleJoinState *) state->join_state;
			MemoryContext oldcontext;

			ResetExprContext(econtext);
			econtext->ecxt_outertuple = slot;

			oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
			
			i = 0;
			foreach(lc, peraggs) {
				OnlineAgg_NumericAggPerAggState *peragg = 
					(OnlineAgg_NumericAggPerAggState *) lfirst(lc);
				
				Datum value;
				bool isNull;
					
				if (peragg->expr_state != NULL) {
					value = ExecEvalExpr(peragg->expr_state, econtext, &isNull, NULL);
				}
				else {
					value = PointerGetDatum(NULL);
					isNull = false;
				}

				peragg->a_transfunc(pergroups[i], peragg,
						value, isNull, join_state->ainfo , 
						join_state->join_state.nrels);

				++i;
			}

			MemoryContextSwitchTo(oldcontext);
			++*nsampled;
		}
		
	}

	else {
		Assert(false); /* unreachable */
	}
}

static TupleTableSlot *
onlineagg_exec_planselection(OnlineAggState *state) {
	OnlineSampleJoin *next_plan = lfirst(state->next_plan_cell);
	TimestampTz start_time, end_time;
	long secs;
	int microsecs;
	int64 time_elapsed;
	uint32 i;
	uint64 nrejected = 0;

	state->next_plan_cell = lnext(state->next_plan_cell);

	start_time = GetCurrentTimestamp();
	
	for (i = 0; i < state->nInitSamples; ++i) {
		TupleTableSlot *slot = ExecProcNode((PlanState *) state->join_state);
		if (NULL == slot) ++nrejected;
	}

	end_time = GetCurrentTimestamp();

	TimestampDifference(start_time, end_time, &secs, &microsecs);
	time_elapsed = ((int64) secs) * 1000ll + ((int64) microsecs) / 1000ll;
	onlineagg_output_planselection(state, next_plan, nrejected, time_elapsed);

	if (state->best_plan == NULL || state->nrejected_best_plan > nrejected) {
		state->best_plan = next_plan;
		state->nrejected_best_plan = nrejected;
	}

	if (state->next_plan_cell == NULL) {
		state->join_state = online_reinit_join(state->join_state, state->best_plan);
		state->join_state->forOutputs = true;
	}
	else {
		next_plan = lfirst(state->next_plan_cell);
		state->join_state = online_reinit_join(state->join_state, next_plan);
	}

	return state->ps.ps_ResultTupleSlot;
}

static void
onlineagg_output_planselection(OnlineAggState *state,
							   OnlineSampleJoin *join_plan,
							   uint64 nrejected,
							   int64 time_elapsed) {
	TupleTableSlot *slot = state->ps.ps_ResultTupleSlot;
	int natts = slot->tts_tupleDescriptor->natts;
	int i;
	
	ExecClearTuple(slot);

	slot->tts_values[0] = Int32GetDatum(join_plan->no);
	slot->tts_isnull[0] = false;

	slot->tts_values[1] = Int64GetDatumFast(time_elapsed);
	slot->tts_isnull[1] = false;

	slot->tts_values[2] = Int64GetDatumFast(state->nInitSamples);
	slot->tts_isnull[2] = false;

	slot->tts_values[3] = Int64GetDatumFast(nrejected);
	slot->tts_isnull[3] = false;

	for (i = 4; i < natts; ++i) {
		slot->tts_values[i] = PointerGetDatum(NULL);
		slot->tts_isnull[i] = true;
	}

	ExecStoreVirtualTuple(slot);
}

TupleTableSlot *
ExecOnlineSampleJoin(OnlineSampleJoinState *state) {
	uint32 nrels,
		   i;
	
	nrels = state->nrels;

	ResetExprContext(state->rel_qual_ctx);
	
	/* extract sample */
	for (i = 1; i <= nrels; ++i) {
		if (NULL == online_sample_from_btree(state, i)) {
			return NULL;
		}
	}

	online_sample_join_set_outputs(state);
	return state->outputs;
}

/**
 * fill in the output tuple table slot 
 *
 * */
static void online_sample_join_set_outputs(OnlineSampleJoinState *state) {
	uint32 i = 1;
	ListCell *lc;
	TupleTableSlot *outputs = state->outputs;
	List *output_expr_states = state->output_expr_states;
	ExprContext *econtext = state->rel_qual_ctx;
	MemoryContext oldcontext;
	
	if (!state->forOutputs) return ;
	
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);		

	ExecClearTuple(outputs);
	foreach(lc, output_expr_states) {
		ExprState *expr_state = (ExprState *) lfirst(lc);
		
		outputs->tts_values[i-1] = ExecEvalExpr(expr_state, econtext,
												&outputs->tts_isnull[i-1],
												NULL);
		++i;
	}
	ExecStoreVirtualTuple(outputs);

	MemoryContextSwitchTo(oldcontext);
}

extern void debug_print_tup(TupleTableSlot * slot);

void debug_print_tups(OnlineSampleJoinState *state);
void debug_print_tups(OnlineSampleJoinState *state) {
	uint32 i, nrels = state->nrels;
	
	for (i = 1; i <= nrels; ++i)
		debug_print_tup(state->tuples[i]);
}

TupleTableSlot *
ExecAdaptiveOnlineSampleJoin(AdaptiveOnlineSampleJoinState *astate) {
	OnlineSampleJoinState *state;
	uint32 nrels,
		   i;
	avlnode **join_path;
	bool has_zero;
	bool rejected;
	
	state = (OnlineSampleJoinState *) astate;
	nrels = state->nrels;

	join_path = astate->join_path;
	
	if (astate->wtree_root == NULL) {
		MemoryContext oldcontext = MemoryContextSwitchTo(astate->wtree_context);
		astate->wtree_root = avl_create_node(NULL, 0.0, NULL);
		MemoryContextSwitchTo(oldcontext);
	}
	join_path[0] = astate->wtree_root;
	
	has_zero = false;
	rejected = false;

	ResetExprContext(state->rel_qual_ctx);
	
	for (i = 1; i < nrels; ++i) {
		TupleTableSlot *slot;
		avlnode *now, *next;
		bool sample_from_all;
		TupleDesc tupdesc;
		uint64 rnd;
		
		astate->cur_rel = i;
		now = join_path[i-1];

		if (now->data) {
			if (now->total == 0) {
				/* this path was explored before, no updates are needed to weights */
				return NULL;
			}
			
			rnd = xql_randint(now->total - 1);	
			sample_from_all = (rnd >= now->seen);
		}
		else {
			sample_from_all = true;
		}
		
		if (sample_from_all) {
			/* from all */

			slot = online_sample_from_btree(state, i);
			now->total = _bt_sample_get_count(state->sample_state[i]);

			if (!slot && now->total != 0) {
				/* this qual is rejected due to rel quals, have to continue anyway */
				slot = state->tuples[i];
				rejected = true;
			}
			
			if (!slot) {
				/* `return NULL` is a subtle bug since the weights 
				 * have not been updated yet. */
				Assert(now->total == 0 && now->data == NULL);
				now->data = now;	/* this node should never be sampled later unless it's the root */
				now->seen = 0;
				has_zero = true;
				break;
			}
			else {
				next = avl_find(now->data, slot, online_avltree_cmp, (void *) astate);

				if (next) {
					/* old tuple */
					astate->ainfo[i].n_total = now->total;
					astate->ainfo[i].n_seen = now->seen;
					astate->ainfo[i].last_was_seen = true;
					astate->ainfo[i].weight = next->weight;
					astate->ainfo[i].sum_weight = now->data->sum;
				}
				else {
					/* new tuple */
					MemoryContext oldcontext;
					TupleTableSlot *key_slot;
					HeapTuple htup_copy;

					astate->ainfo[i].n_total = now->total;
					astate->ainfo[i].n_seen = now->seen;
					astate->ainfo[i].last_was_seen = false;
					
					oldcontext = MemoryContextSwitchTo(astate->wtree_context);

					++now->seen;
					key_slot = ExecAllocTableSlot(&astate->slots);
					

					/* don't pin the tupdesc for it's already pinned by state->tuples, which
					 * has the same live period; this will save us a lot of time of releasing
					 * the tupdescs */
					tupdesc = RelationGetDescr(state->heaprels[i]);
					ExecSetSlotDescriptor_nopin(key_slot, tupdesc);
					
					htup_copy = ExecCopySlotTuple(slot);
					ExecStoreTuple(htup_copy, key_slot, InvalidBuffer, true);

					next = avl_create_node(key_slot, 0.0, NULL);
					now->data = avl_insert(now->data, next, 
							online_avltree_cmp, (void *) astate);
					
					MemoryContextSwitchTo(oldcontext);
				}
			}
		}

		else {
			/* from seen */
			HeapTuple htup;	
			double rnd;
			
			if (now->data->sum == 0) {
				/* all tuples that are previously seen cannot join */
				has_zero = true;
				break;
			}

			rnd = xql_drand() * now->data->sum;

			Assert(now->seen != 0);

			next = avl_find_weight(now->data, rnd);

			Assert(next);
			Assert(next->weight != 0.0 || next->total == 0);
			
			astate->ainfo[i].n_total = now->total;
			astate->ainfo[i].n_seen = now->seen;
			astate->ainfo[i].last_was_seen = true;
			astate->ainfo[i].weight = next->weight;
			astate->ainfo[i].sum_weight = now->data->sum;
			
			/* store the tuple */
			htup = ExecFetchSlotTuple(next->slot);
			ExecStoreTuple(htup, state->tuples[i], InvalidBuffer, false);
		}

		join_path[i] = next;
	}

	/* the sample of the last relation is always sampled from the index */
	if (!has_zero) {
		TupleTableSlot *slot;

		++astate->cur_rel;
		Assert(astate->cur_rel = nrels);
		slot = online_sample_from_btree(state, nrels);
		join_path[nrels - 1]->total = 
			_bt_sample_get_count(state->sample_state[nrels]);
		
		if (slot == NULL) {
			if (join_path[nrels - 1]->total != 0) {
				rejected = true;
			}
			else {
				has_zero = true;
			}
		}
		else {

			astate->ainfo[i].n_total = join_path[nrels - 1]->total;
		}
	}

	/* fix the weights along the join path */
	if (has_zero) {
		if (astate->cur_rel > 1)
			avl_maintain_sum(join_path[astate->cur_rel - 1], 0.0);
	}
	else {
		Assert(astate->cur_rel == nrels);
		avl_maintain_sum(join_path[nrels - 1], join_path[nrels - 1]->total);
	}
	
	if (astate->cur_rel > 2) {
		for (i = astate->cur_rel - 2; i > 0; --i) {
			double sum = join_path[i]->data->sum;
			double new_weight;
			
			if (sum == 0) {
				new_weight = join_path[i]->total - join_path[i]->seen;
			}
			else {
				new_weight = sum * join_path[i]->total / join_path[i]->seen;
			}

			avl_maintain_sum(join_path[i], new_weight);
		}
	}

	{
		/* (void) validate_tree(astate->wtree_root->data); */
	}

	/* if there's rejected tuples, return NULL */
	if (has_zero || rejected) {
		return NULL;
	}
	
	online_sample_join_set_outputs(state);
	return state->outputs;
}

static TupleTableSlot *
online_sample_from_btree(OnlineSampleJoinState *state, int i) {
	Relation index = state->join_index[i];
	BTSampleState sstate;
	int n_runtime_keys = state->n_runtime_keys[i];
	IndexRuntimeKeyInfo *runtime_keys = state->runtime_keys[i];
	ExprContext *econtext = state->rel_qual_ctx;

	/* first invocation; need to initialize BTSampleState */
	if (state->sample_state[i] == NULL) {
		MemoryContext oldcontext = MemoryContextSwitchTo(state->perPlanContext);
		state->sample_state[i] = _bt_init_sample_state(index, false);
		MemoryContextSwitchTo(oldcontext);
	}
	sstate = state->sample_state[i];
	
	/* evaluate runtime keys */
	if (n_runtime_keys > 0) {
		online_eval_runtime_keys(econtext, runtime_keys, n_runtime_keys);
		_bt_sample_set_key(sstate, state->n_joinkeys[i], state->join_keys[i]);
	}
	else {
		/* no runtime keys but this is the sstate has not been set to the keys */
		if (!_bt_sample_key_is_ok(sstate)) {
			_bt_sample_set_key(sstate, state->n_joinkeys[i], state->join_keys[i]);
		}
	}
		
	if (!_bt_sample_key_is_ok(sstate)) {
		ereport(ERROR, (errmsg("scankey error")));		
	}
	
	if (_bt_sample(sstate)) {
		uint64 count = _bt_sample_get_count(sstate);
		HeapTuple htup;

		if (count == 0) 
			return NULL;
		state->inv_prob[i] = count;
		
		htup = online_fetch_heap(state, sstate, i);	
		Assert(htup != NULL);

		ExecStoreTuple(htup,
					   state->tuples[i],
					   state->buf[i],
					   false);
			
		if (list_length(state->rel_quals[i]) != 0) {
				if (!ExecQual(state->rel_quals[i], econtext, false)) {
				return NULL;
			}
		}
	}
	else {
		ereport(ERROR, (errmsg("error when sampling on the btree")));
	}
	
	return state->tuples[i];
}

static void
online_eval_runtime_keys(ExprContext *econtext, IndexRuntimeKeyInfo *runtime_keys,
						 int n_runtime_keys) {
	int i;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	
	for (i = 0; i < n_runtime_keys; ++i) {
		ScanKey scan_key = runtime_keys[i].scan_key;
		ExprState *key_expr = runtime_keys[i].key_expr;
		Datum value;
		bool isNull;

		value = ExecEvalExpr(key_expr, econtext, &isNull, NULL);

		if (isNull) {
			scan_key->sk_argument = value;
			scan_key->sk_flags |= SK_ISNULL;
		}
		else {
			if (runtime_keys[i].key_toastable)
				value = PointerGetDatum(PG_DETOAST_DATUM(value));
			scan_key->sk_argument = value;
			scan_key->sk_flags &= ~SK_ISNULL;
		}
	}

	MemoryContextSwitchTo(oldcontext);
}

static HeapTuple
online_fetch_heap(OnlineSampleJoinState *state, BTSampleState sstate, int i) {
	ItemPointer tid = _bt_sample_get_tid(sstate);
	Buffer	prev_buf = state->buf[i];
	Buffer  cur_buf;
	bool	got_heap_tuple;
	bool	all_dead;
	
	cur_buf = ReleaseAndReadBuffer(prev_buf, state->heaprels[i],
								   ItemPointerGetBlockNumber(tid));
	state->buf[i] = cur_buf;

	if (cur_buf != prev_buf)
		heap_page_prune_opt(state->heaprels[i], cur_buf);
	
	LockBuffer(cur_buf, BUFFER_LOCK_SHARE);
	got_heap_tuple = heap_hot_search_buffer(tid, state->heaprels[i],
											cur_buf,
											state->ps.state->es_snapshot,
											&state->heap_tup[i],
											&all_dead,
											true);
	LockBuffer(cur_buf, BUFFER_LOCK_UNLOCK);

	if (got_heap_tuple) {
		return &state->heap_tup[i];
	}

	ereport(ERROR, (errmsg("tuples in the hot chain are all dead"),
					errdetail("btsample does not support vacuum")));
	return NULL;
}

void
ExecEndOnlineAgg(OnlineAggState *state) {
	
	/* end join state */
	ExecEndOnlineSampleJoin(state->join_state);
	ExecClearTuple(state->ps.ps_ResultTupleSlot);	
	MemoryContextDelete(state->aggcontext);
}

void
ExecEndOnlineSampleJoin(OnlineSampleJoinState *state) {
	uint32 nrels,
		   i;

	nrels = state->nrels;

	if (IsA(state, AdaptiveOnlineSampleJoinState)) {
		/* don't reset these tuple tables, because the ref counts of 
		 * decriptors are not maintained */
		/*
		AdaptiveOnlineSampleJoinState *astate = 
			(AdaptiveOnlineSampleJoinState *) state;

		ExecResetTupleTable(astate->slots, true); */
	}

	for (i = 1; i <= nrels; ++i) {
		ExecClearTuple(state->tuples[i]);
		if (BufferIsValid(state->buf[i]))
			ReleaseBuffer(state->buf[i]);

		Assert(RelationIsValid(state->heaprels[i]));
		Assert(RelationIsValid(state->join_index[i]));
		heap_close(state->heaprels[i], NoLock);
		index_close(state->join_index[i], NoLock);
	}
	ExecClearTuple(state->outputs);
}

static int 
online_avltree_cmp(TupleTableSlot *slot1, TupleTableSlot *slot2, void *cxt) {
	AdaptiveOnlineSampleJoinState *astate = (AdaptiveOnlineSampleJoinState *) cxt;
	
	uint32 cur_rel = astate->cur_rel;
	int j;
	int ncols = astate->pk_ncolumns[cur_rel];

	for (j = 0; j < ncols; ++j) {
		int attno = astate->pk_attno[cur_rel][j];
		Oid collation = astate->pk_collation[cur_rel][j];
		FmgrInfo *fmgrinfo = &astate->pk_fmgrinfo[cur_rel][j];
		
		Datum d1, d2;
		bool isnull1, isnull2;

		d1 = slot_getattr(slot1, attno, &isnull1);
		d2 = slot_getattr(slot2, attno, &isnull2);
	

		/* null <any non-null value */
		if (isnull1) {
			if (isnull2)
				continue;
			else
				return -1;
		}
		else if (isnull2) {
			return 1;	
		}
		else {
			
			int32 cmp_res = DatumGetInt32(FunctionCall2Coll(fmgrinfo,
															collation,
														    d1,
															d2));
			if (0 != cmp_res)
				return cmp_res;
		}
	}

	return 0;
}


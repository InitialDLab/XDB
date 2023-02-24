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

typedef struct OnlineAggHashEntryData *OnlineAggHashEntry;

typedef struct OnlineAggHashEntryData {
	TupleHashEntryData shared;
	
	uint64 nsampled;
	OnlineAgg_NumericAggPerGroupState *pergroups[1];
} OnlineAggHashEntryData;


static void onlineagg_init_pergroups(
		OnlineAggState *state,
        List *peraggs,
		OnlineAgg_NumericAggPerGroupState **pergroups);
static void 
onlineagg_init_partial_pergroups(
    OnlineAggState *state,
    List *peraggs,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups);
static void init_online_sample_join(OnlineSampleJoin *node, EState *estate, int eflags,
									OnlineSampleJoinState *state);
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
                            List *peraggs,
							OnlineAgg_NumericAggPerGroupState **pergroups,
							uint64 *nsampled,
                            uint64 *ntotal_sampled);
static void onlineagg_reset_partial_pergroups(
    OnlineAggState *state,
    List *peraggs,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups);
static void onlineagg_advance_partial_pergroups(
        OnlineAggState *state, 
		TupleTableSlot *slot,
        List *peraggs,
		OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups);
static void onlineagg_add_partial_pergroups_to_pergroups(
    OnlineAggState *state,
    List *peraggs,
    OnlineAgg_NumericAggPerGroupState **pergroups,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups,
    uint64 *nsampled,
    uint64 *ntotal_sampled);
static TupleTableSlot *onlineagg_exec_planselection(OnlineAggState *state);
static void onlineagg_output_planselection(OnlineAggState *state,
							   OnlineSampleJoin *join_plan,
                               uint64 ntotal_sample,
							   uint64 nrejected,
                               double estimator_variance,
							   int64 time_elapsed);
static void online_sample_join_set_outputs(OnlineSampleJoinState *state);
static TupleTableSlot *online_sample_from_btree(OnlineSampleJoinState *state, int i);
static void online_eval_runtime_keys(ExprContext *econtext, 
							IndexRuntimeKeyInfo *runtime_keys,
							int n_runtime_keys);
static HeapTuple online_fetch_heap(OnlineSampleJoinState *state, ItemPointer tid, int i);

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
    aggstate->push_down_agg = node->push_down_agg;
    aggstate->push_down_filter = node->push_down_filter;

	aggstate->aggcontext = AllocSetContextCreate(CurrentMemoryContext, "OnlineAggContext",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);
	
	aggstate->nInitSamples = estate->es_plannedstmt->nInitSamples;
	if (aggstate->nInitSamples > 0) {
		OnlineSampleJoin *join_plan = linitial(node->candidate_join_plans);
		aggstate->next_plan_cell = list_head(node->candidate_join_plans);
		aggstate->best_plan = NULL;
		aggstate->variance_best_plan = XQL_DOUBLE_INF;
		aggstate->join_state = ExecInitOnlineSampleJoin(join_plan, estate, eflags);
	}
	else {
		OnlineSampleJoin *join_plan = linitial(node->candidate_join_plans);
		aggstate->next_plan_cell = NULL;
		aggstate->join_state = ExecInitOnlineSampleJoin(join_plan, estate, eflags);
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
		onlineagg_init_pergroups(aggstate, aggstate->peraggs, aggstate->pergroups);

        if (node->push_down_agg) {
            aggstate->partial_pergroups = (OnlineAgg_PartialNumericAggPerGroupState **)
                palloc0(sizeof(OnlineAgg_PartialNumericAggPerGroupState *) * aggstate->naggs);
            onlineagg_init_partial_pergroups(aggstate, aggstate->peraggs, aggstate->partial_pergroups);
        }
	}
	aggstate->ntotal_sampled = 0;
	
	onlineagg_initialize_resulttupleslot(node, estate, aggstate);

	return aggstate;
}

static void onlineagg_init_pergroups(
		OnlineAggState *state,
        List *peraggs,
		OnlineAgg_NumericAggPerGroupState **pergroups) {
	
	ListCell *lc;
	int i = 0;

	foreach(lc, peraggs) {
		OnlineAgg_NumericAggPerAggState *peragg = 
			(OnlineAgg_NumericAggPerAggState *) lfirst(lc);

		pergroups[i++] = peragg->initfunc(state->aggcontext, peragg);
	}
}

static void 
onlineagg_init_partial_pergroups(
    OnlineAggState *state,
    List *peraggs,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups) {
    
    ListCell *lc;
    int i = 0;

    foreach(lc, peraggs) {
        OnlineAgg_NumericAggPerAggState *peragg = 
            (OnlineAgg_NumericAggPerAggState *) lfirst(lc);
        partial_pergroups[i++] = peragg->partial_initfunc(state->aggcontext, peragg);
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
        state->len_rel_quals = (int *) palloc(sizeof(int) * (state->nrels + 1));
		state->runtime_keys = (IndexRuntimeKeyInfo **)
								palloc(sizeof(IndexRuntimeKeyInfo *) * (state->nrels + 1));
		state->n_runtime_keys = (int *) palloc(sizeof(int) * (state->nrels + 1));
		
		state->rel_qual_ctx = CreateExprContext(estate);
		state->rel_qual_ctx->ecxt_reltuples = state->tuples;
		state->sample_state = (BTSampleState *) palloc0(sizeof(BTSampleState) * (state->nrels + 1));
		state->inv_prob = (double *) palloc(sizeof(double) * (state->nrels + 1));

		state->buf = (Buffer *) palloc(sizeof(Buffer) * (state->nrels + 1));
		state->heap_tup = (HeapTupleData *) palloc(sizeof(HeapTupleData) * (state->nrels + 2));
		
		state->tuples[0] = NULL;
		state->heaprels[0] = InvalidRelation;
		state->buf[0] = InvalidBuffer;
		state->join_index[0] = InvalidRelation;

		state->n_joinkeys[0] = 0;
		state->join_keys[0] = NULL;
		state->rel_quals[0] = NIL;
        state->len_rel_quals[0] = 0;
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
    state->fetch_all_from_last = node->fetch_all_from_last;
    state->fetch_ready = false;
    state->sample_from_filtered = node->sample_from_filtered;
	
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
        int len_rel_quals;

		/* init join quals */
		state->n_runtime_keys[i] = 0;
		online_ExecBuildScanKeys((PlanState  *) state, 
								 state->join_index[i],
								 node->join_quals[i], 
								 &state->join_keys[i], &state->n_joinkeys[i],
								 &state->runtime_keys[i], &state->n_runtime_keys[i]);
		
		/* initialize rel quals */
        len_rel_quals = 0;
		foreach(lc, node->rel_quals[i]) {
			Expr *qual = (Expr *) lfirst(lc);

			expr_state = online_ExecInitExpr(qual, (PlanState *) state,
											 online_relqual_scalarVarEval);
			rel_quals = lappend(rel_quals, expr_state);
            len_rel_quals++;
		}
		state->rel_quals[i] = rel_quals;
        state->len_rel_quals[i] = len_rel_quals;
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

OnlineSampleJoinState *
ExecInitOnlineSampleJoin(OnlineSampleJoin *node, EState *estate, int eflags) {
    OnlineSampleJoinState *state;

    state = makeNode(OnlineSampleJoinState);
    state->initialized = false;

    init_online_sample_join(node, estate, eflags, state);
    return state;
}

static OnlineSampleJoinState *
online_reinit_join(OnlineSampleJoinState *state, OnlineSampleJoin *node) {

	init_online_sample_join(node, state->ps.state, 0, state);
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
	int ncols = withPlanSelection * 2 + 3 + aggstate->naggs + ntargets;
	
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

    if (withPlanSelection)
        TupleDescInitEntry(tupdesc, i++, "variance", FLOAT8OID, 0, 0);

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
		
			onlineagg_advance_pergroups(state, slot, state->peraggs,
										entry->pergroups,
										&entry->nsampled,
                                        &state->ntotal_sampled);
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
		onlineagg_init_pergroups(state, state->peraggs, entry->pergroups);
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

        if (state->push_down_agg && state->join_state->fetch_ready) {
            /* If we see no non null tuple in the leaf level, 
             * it is essentially treated as a rejected sample. */
            bool seen_non_null = false;
            do {
                if (NULL != slot) {
                    if (!seen_non_null) {
                        onlineagg_reset_partial_pergroups(
                            state, state->peraggs, state->partial_pergroups);
                        seen_non_null = true;
                    }
                    onlineagg_advance_partial_pergroups(
                        state, slot, state->peraggs, state->partial_pergroups);
                }
		        slot = ExecProcNode((PlanState *) state->join_state);
            } while (state->join_state->fetch_ready);

            if (seen_non_null) {
                onlineagg_add_partial_pergroups_to_pergroups(state, state->peraggs,
                        state->pergroups, state->partial_pergroups,
                        &state->nsampled, &state->ntotal_sampled);
            } else {
                onlineagg_advance_pergroups(state, NULL, state->peraggs,
                        state->pergroups,
                        &state->nsampled,
                        &state->ntotal_sampled);
            }
        } else {
            /* either we are not doing agg at the leaf level or 
             * there are no tuples in the final level (a rejection) */
            onlineagg_advance_pergroups(state, slot, state->peraggs,
                                        state->pergroups,
                                        &state->nsampled,
                                        &state->ntotal_sampled);
        }
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
	slot->tts_values[i] = Int64GetDatumFast(state->ntotal_sampled);
	slot->tts_isnull[i++] = false;

    /* column nrejected */
    slot->tts_values[i] = Int64GetDatumFast(state->ntotal_sampled - nsampled);
    slot->tts_isnull[i++] = false;

    if (state->nInitSamples > 0) {
        slot->tts_values[i] = PointerGetDatum(NULL);
        slot->tts_isnull[i++] = true;
    }

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
                            List *peraggs,
							OnlineAgg_NumericAggPerGroupState **pergroups,
							uint64 *nsampled,
                            uint64 *ntotal_sampled) {
	
	int i;
	ListCell *lc;
	ExprContext *econtext = state->expr_ctx;;
    
    ++*ntotal_sampled;
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

static void
onlineagg_reset_partial_pergroups(
    OnlineAggState *state,
    List *peraggs,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups) {
    
	int i = 0;
	ListCell *lc;
    
    foreach(lc, peraggs) {
        OnlineAgg_NumericAggPerAggState *peragg = 
            (OnlineAgg_NumericAggPerAggState *) lfirst(lc);
        peragg->partial_resetfunc(partial_pergroups[i], peragg);
        ++i;
    }
}

static void
onlineagg_advance_partial_pergroups(
    OnlineAggState *state, 
    TupleTableSlot *slot,
    List *peraggs,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups) {

	int i;
	ListCell *lc;
	ExprContext *econtext = state->expr_ctx;
    MemoryContext oldcontext;

    Assert(NULL != slot);
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

        peragg->partial_transfunc(partial_pergroups[i], peragg, value, isNull);

        ++i;
    }

    MemoryContextSwitchTo(oldcontext);
}

static void
onlineagg_add_partial_pergroups_to_pergroups(
    OnlineAggState *state,
    List *peraggs,
    OnlineAgg_NumericAggPerGroupState **pergroups,
    OnlineAgg_PartialNumericAggPerGroupState **partial_pergroups,
    uint64 *nsampled,
    uint64 *ntotal_sampled) {
    
    OnlineSampleJoinState *join_state = state->join_state;
	int i = 0;
	ListCell *lc;
    
    foreach(lc, peraggs) {
        OnlineAgg_NumericAggPerAggState *peragg = 
            (OnlineAgg_NumericAggPerAggState *) lfirst(lc);
        peragg->partial_finalfunc(
            partial_pergroups[i], pergroups[i], peragg,
            join_state->inv_prob, join_state->nrels);
        ++i;
    }
    ++*nsampled;
    ++*ntotal_sampled;
}

static TupleTableSlot *
onlineagg_exec_planselection(OnlineAggState *state) {
	OnlineSampleJoin *next_plan = lfirst(state->next_plan_cell);
	TimestampTz start_time, end_time, now_time;
	long secs;
	int microsecs;
	int64 time_elapsed;
	uint32 i;
    uint64 ntotal_sampled = 0;
	uint64 nsampled = 0;
    uint64 nrejected;
    List *peraggs = lappend(NIL, linitial(state->peraggs));
    OnlineAgg_NumericAggPerGroupState *pergroups[1];
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroups[1];
    double sample_variance;
    double estimator_variance;
    
    /* XXX Do we need to maintain these pergroups/partial_pergroups in a
     * temporary memroy context? */
	state->next_plan_cell = lnext(state->next_plan_cell);
    onlineagg_init_pergroups(state, peraggs, pergroups);
    if (state->push_down_agg) {
        onlineagg_init_partial_pergroups(state, peraggs, partial_pergroups);
    }

	now_time = start_time = GetCurrentTimestamp();
    end_time = TimestampTzPlusMilliseconds(start_time, 3000);
	
	for (i = 0; i < state->nInitSamples; ++i) {
        TupleTableSlot *slot;
        
        slot = ExecProcNode((PlanState *) state->join_state);

        if (state->push_down_agg && state->join_state->fetch_ready) {
            /* If we see no non null tuple in the leaf level, 
             * it is essentially treated as a rejected sample. */
            bool seen_non_null = false;
            do {
                if (NULL != slot) {
                    if (!seen_non_null) {
                        onlineagg_reset_partial_pergroups(
                            state, peraggs, partial_pergroups);
                        if (!state->hasGrouping) {
                            onlineagg_reset_partial_pergroups(
                                state, state->peraggs, state->partial_pergroups);
                        }
                        seen_non_null = true;
                    }
                    onlineagg_advance_partial_pergroups(
                        state, slot, peraggs, partial_pergroups);
                    if (!state->hasGrouping) {
                        onlineagg_advance_partial_pergroups(
                            state, slot, state->peraggs, state->partial_pergroups);
                    }
                }
		        slot = ExecProcNode((PlanState *) state->join_state);
            } while (state->join_state->fetch_ready);

            if (seen_non_null) {
                onlineagg_add_partial_pergroups_to_pergroups(state, peraggs,
                        pergroups, partial_pergroups, &nsampled, &ntotal_sampled);
                if (!state->hasGrouping) {
                    onlineagg_add_partial_pergroups_to_pergroups(state, state->peraggs,
                        state->pergroups, state->partial_pergroups,
                        &state->nsampled, &state->ntotal_sampled);
                }
            } else {
                onlineagg_advance_pergroups(state, NULL, peraggs, pergroups, 
                        &nsampled, &ntotal_sampled);
                if (!state->hasGrouping) {
                    onlineagg_advance_pergroups(state, NULL, state->peraggs,
                        state->pergroups,
                        &state->nsampled,
                        &state->ntotal_sampled);
                }
            }
        } else {
            /* either we are not doing agg at the leaf level or 
             * there are no tuples in the final level (a rejection) */
            onlineagg_advance_pergroups(state, slot, peraggs, pergroups,
                                        &nsampled, &ntotal_sampled);
            if (!state->hasGrouping) {
               onlineagg_advance_pergroups(state, slot, state->peraggs,
                       state->pergroups, &state->nsampled, &state->ntotal_sampled);
            }
        }

        if ((now_time = GetCurrentTimestamp()) >= end_time) break;
	}

	TimestampDifference(start_time, now_time, &secs, &microsecs);
	time_elapsed = ((int64) secs) * 1000ll + ((int64) microsecs) / 1000ll;
    calc_variance_double(pergroups[0], &sample_variance, ntotal_sampled);
    if (sample_variance == 0.0) {
        sample_variance = XQL_DOUBLE_INF;
    }
    estimator_variance = sample_variance * ((double) time_elapsed / ntotal_sampled);
    nrejected = ntotal_sampled - nsampled;

	onlineagg_output_planselection(state, next_plan, ntotal_sampled,
            nrejected, estimator_variance, time_elapsed);

	if (state->best_plan == NULL || (
        ntotal_sampled >= state->ntotal_sampled_best_plan * 2 ||
        (ntotal_sampled >= state->ntotal_sampled_best_plan * 0.5 &&
        //(nsampled * 1.0 / ntotal_sampled >= 1.5 * state->naccepted_best_plan / state->ntotal_sampled_best_plan ||
        //(nsampled * 1.0 / ntotal_sampled >= 0.8 * state->naccepted_best_plan / state->ntotal_sampled_best_plan &&
        state->variance_best_plan > estimator_variance))) {

        state->best_plan = next_plan;
		state->variance_best_plan = estimator_variance;
        state->naccepted_best_plan = nsampled;
        state->ntotal_sampled_best_plan = ntotal_sampled;
	}

	if (state->next_plan_cell == NULL) {
		state->join_state = online_reinit_join(state->join_state, state->best_plan);
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
                               uint64 ntotal_sample,
							   uint64 nrejected,
                               double estimator_variance,
							   int64 time_elapsed) {
	TupleTableSlot *slot = state->ps.ps_ResultTupleSlot;
	int natts = slot->tts_tupleDescriptor->natts;
	int i;
	
	ExecClearTuple(slot);

	slot->tts_values[0] = Int32GetDatum(join_plan->no);
	slot->tts_isnull[0] = false;

	slot->tts_values[1] = Int64GetDatumFast(time_elapsed);
	slot->tts_isnull[1] = false;

	slot->tts_values[2] = Int64GetDatumFast(ntotal_sample);
	slot->tts_isnull[2] = false;

	slot->tts_values[3] = Int64GetDatumFast(nrejected);
	slot->tts_isnull[3] = false;

    slot->tts_values[4] = Float8GetDatumFast(estimator_variance);
    slot->tts_isnull[4] = false;

	for (i = 5; i < natts; ++i) {
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
    if (!state->fetch_all_from_last || !state->fetch_ready) {
        for (i = 1; i <= nrels; ++i) {
            if (NULL == online_sample_from_btree(state, i)) {
                return NULL;
            }
        }
    } else {
        if (NULL == online_sample_from_btree(state, nrels)) {
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

static TupleTableSlot *
online_sample_from_btree(OnlineSampleJoinState *state, int i) {
	Relation index = state->join_index[i];
	BTSampleState sstate;
	int n_runtime_keys = state->n_runtime_keys[i];
	IndexRuntimeKeyInfo *runtime_keys = state->runtime_keys[i];
	ExprContext *econtext = state->rel_qual_ctx;
    bool should_do_sample;
    HeapTuple htup;
    ItemPointer tid;
    bool is_last = i == state->nrels;

	/* first invocation; need to initialize BTSampleState */
	if (state->sample_state[i] == NULL) {
		MemoryContext oldcontext = MemoryContextSwitchTo(state->perPlanContext);
		state->sample_state[i] = _bt_init_sample_state(index, false,
                is_last ? state->fetch_all_from_last : 
                (state->sample_from_filtered && state->len_rel_quals[i] != 0));
		MemoryContextSwitchTo(oldcontext);
	}
	sstate = state->sample_state[i];
    
    should_do_sample = !state->fetch_all_from_last || !is_last || 
            (is_last && !state->fetch_ready);

    if (should_do_sample) {
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
        
        if (!_bt_sample(sstate)) {
            ereport(ERROR, (errmsg("error when sampling on the btree")));
        }

        if (is_last) state->fetch_ready = true;
        state->inv_prob[i] = _bt_sample_get_inverse_probability(sstate);
    }
    
    if (!is_last && state->sample_from_filtered && state->len_rel_quals[i] != 0) {
        uint64 n_matched = 0;
        ItemPointerData sampled_tid;
        uint64 r;
        for (tid = _bt_sample_get_next_tid(sstate); ItemPointerIsValid(tid);
             tid = _bt_sample_get_next_tid(sstate)) {
            htup = online_fetch_heap(state, tid, i);
            Assert(htup != NULL);
            ExecStoreTuple(htup, state->tuples[i], state->buf[i], false); 

            if (ExecQual(state->rel_quals[i], econtext, false)) {
                n_matched += 1;
                if (n_matched == 1) {
                    sampled_tid = *tid;
                } else {
                    r = xql_randint(n_matched - 1);
                    if (r == 0) {
                        sampled_tid = *tid;
                    }
                }
            }
        }
        if (n_matched == 0) {
            return NULL;
        } else {
            htup = online_fetch_heap(state, &sampled_tid, i);
            ExecStoreTuple(htup, state->tuples[i], state->buf[i], false);
            state->inv_prob[i] *= (double) n_matched;
        }
    } else {
        if (is_last && state->fetch_all_from_last) {
            tid = _bt_sample_get_next_tid(sstate); 
            if (!ItemPointerIsValid(tid)) {
                state->fetch_ready = false;
                return NULL;
            }
        } else {
           tid = _bt_sample_get_tid(sstate);
           if (!ItemPointerIsValid(tid)) {
               return NULL;
           }
        }

        htup = online_fetch_heap(state, tid, i);	
        Assert(htup != NULL);
        ExecStoreTuple(htup, state->tuples[i], state->buf[i], false);
                    
        if (state->len_rel_quals[i] != 0) {
            if (!ExecQual(state->rel_quals[i], econtext, false)) {
                return NULL;
            }
        }
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
online_fetch_heap(OnlineSampleJoinState *state, ItemPointer tid, int i) {
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


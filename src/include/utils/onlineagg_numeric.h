/*-----------------------------------------------------------
 *
 * onlineagg_numeric.h
 *     Online aggregation numeric types and routines.
 *
 * Copyright (c) 2015-2017, InitialD Lab
 *
 * src/include/utils/onlineagg_numeric.h
 *
 *-----------------------------------------------------------
 */

#ifndef _PG_ONLINEAGG_NUMERIC_H_
#define _PG_ONLINEAGG_NUMERIC_H_

#include "numeric.h"
#include "nodes/plannodes.h"

typedef struct OnlineAgg_adaptive_info_t {
	uint64 n_total;		/* the number of tuples that join */
	uint64 n_seen;		/* the number of tuples that join and were sampled at least once */
	
	bool last_was_seen;	/* whether the last sample was seen before */
	double weight;		/* the weight of the last sample (set when last_was_seen == true) */
	double sum_weight;	/* the sum of weights of seen tuples (set when last_was_seen == true) */
} OnlineAgg_adaptive_info_t;

/* for online sample join aggregations. These should be somewhere else,
 * but we need to put them here to access the static functions in numeric.c */ 
/* inv_prob start from 1*/
typedef struct NumericAggState OnlineAgg_NumericAggPerGroupState;
typedef struct OnlineAgg_NumericAggPerAggState OnlineAgg_NumericAggPerAggState;

typedef OnlineAgg_NumericAggPerGroupState *
		(*OnlineAgg_NumericAgg_initfunc)(
				MemoryContext aggcontext,
				OnlineAgg_NumericAggPerAggState *peragg);

typedef void (*OnlineAgg_NumericAgg_transfunc)(
		OnlineAgg_NumericAggPerGroupState *pergroup,
		OnlineAgg_NumericAggPerAggState *peragg,
		Datum value,
		bool isZero,	/* is this sample rejected? */
		const uint64 *inv_prob,
		int nrels);

typedef void (*OnlineAgg_NumericAgg_adaptive_transfunc)(
		OnlineAgg_NumericAggPerGroupState *pergroup,
		OnlineAgg_NumericAggPerAggState *peragg,
		Datum value,
		bool isZero,
		OnlineAgg_adaptive_info_t *ainfo,
		int nrels);


typedef void (*OnlineAgg_NumericAgg_finalfunc)(
		OnlineAgg_NumericAggPerGroupState *pergroup, 
		OnlineAgg_NumericAggPerAggState *peragg,
	    double confidence,
		uint64 N,
		Datum *p_result,
		Datum *p_rel_ci);

extern OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState(Expr *expr);

#endif

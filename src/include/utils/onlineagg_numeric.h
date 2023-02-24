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

/* for online sample join aggregations. These should be somewhere else,
 * but we need to put them here to access the static functions in numeric.c */ 
/* index of inv_prob starts from 1*/
typedef struct NumericAggState OnlineAgg_NumericAggPerGroupState;
typedef struct NumericAggState OnlineAgg_PartialNumericAggPerGroupState;
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
		const double *inv_prob,
		int nrels);

typedef void (*OnlineAgg_NumericAgg_finalfunc)(
		OnlineAgg_NumericAggPerGroupState *pergroup, 
		OnlineAgg_NumericAggPerAggState *peragg,
	    double confidence,
		uint64 N,
		Datum *p_result,
		Datum *p_rel_ci);

typedef OnlineAgg_PartialNumericAggPerGroupState *
        (*OnlineAgg_PartialNumericAgg_initfunc)(
        MemoryContext aggcontext,
        OnlineAgg_NumericAggPerAggState *peragg);

typedef void (*OnlineAgg_PartialNumericAgg_resetfunc)(
        OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
        OnlineAgg_NumericAggPerAggState *peragg);

typedef void (*OnlineAgg_PartialNumericAgg_transfunc)(
		OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
		OnlineAgg_NumericAggPerAggState *peragg,
		Datum value,
		bool isZero);

typedef void (*OnlineAgg_PartialNumericAgg_finalfunc)(
		OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup, 
		OnlineAgg_NumericAggPerGroupState *pergroup, 
		OnlineAgg_NumericAggPerAggState *peragg,
		const double *inv_prob,
		int nrels);

extern OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState(Expr *expr);

extern void
calc_variance_double(
        OnlineAgg_NumericAggPerGroupState *pergroup,
        double *p_variance,
        uint64 N);
#endif

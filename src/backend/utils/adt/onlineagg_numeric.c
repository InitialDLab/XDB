/*-------------------------------------------------------------------
 * 
 * onlineagg_numeric.c
 *     online aggregation types and routines
 *
 * Copyright (c) 2015-2017, InitialD Lab
 *
 * IDENTIFICATION
 *     src/backend/utils/adt/onlineagg_numeric.c
 *
 *-------------------------------------------------------------------
 */

#ifndef ONLINEAGG_NUMERIC_C_INCLUDED_FROM_NUMERIC_C
#	error "utils/onlineagg_numeric.c has to be compiled with numeric.c"
#endif

#include "utils/xql_math.h"
#include "nodes/execnodes.h"

typedef struct OnlineAgg_NumericAggPerAggState_sum {
	OnlineAgg_NumericAggPerAggState state;
	
	PGFunction tonumeric;
} OnlineAgg_NumericAggPerAggState_sum;

typedef struct OnlineAgg_NumericAggState_count {
	OnlineAgg_NumericAggPerAggState state;
	
	Numeric scale;
} OnlineAgg_NumericAggPerAggState_count;

static NumericAggState *
makeNumericAggStateFromMemoryContext(MemoryContext agg_context, bool calcSumX2);

static OnlineAgg_NumericAggPerGroupState *
init_OnlineAgg_NumericAggPerGroupState(MemoryContext aggcontext,
									   OnlineAgg_NumericAggPerAggState *peragg);
static OnlineAgg_PartialNumericAggPerGroupState *
init_OnlineAgg_PartialNumericAggPerGroupState(MemoryContext aggcontext,
                                              OnlineAgg_NumericAggPerAggState *peragg);

static void
reset_OnlineAgg_PartialNumericAggPerGroupState(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerAggState *peragg);

static PGFunction OnlineAgg_get_tonumeric_func(Oid type);

static OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState_sum(OnlineAggSum *sum);

static OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState_count(OnlineAggCount *count);

static void trans_OnlineAgg_sum(OnlineAgg_NumericAggPerGroupState *pergroup,
								OnlineAgg_NumericAggPerAggState *peragg,
								Datum value,
								bool isZero,
								const double *inv_prob,
								int nrels);
static void
trans_OnlineAgg_Partial_sum(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerAggState *peragg,
    Datum value,
    bool isZero);
static void trans_OnlineAgg_count(OnlineAgg_NumericAggPerGroupState *pergroup,
								  OnlineAgg_NumericAggPerAggState *peragg,
								  Datum value,
								  bool isZero,
								  const double *inv_prob,
								  int nrels);
static void
trans_OnlineAgg_Partial_count(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerAggState *peragg,
    Datum value,
    bool isZero);
static void trans_OnlineAgg_common(OnlineAgg_NumericAggPerGroupState *pergroup,
								   Numeric value,
								   const double *inv_prob,
								   int nrels);
static void
trans_OnlineAgg_Partial_common(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    Numeric value);
static void
final_OnlineAgg_Partial_common(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerGroupState *pergroup,
    OnlineAgg_NumericAggPerAggState *peragg,
    const double *inv_prob,
    int nrels);
static void calc_mean_variance(
        OnlineAgg_NumericAggPerGroupState *pergroup,
        NumericVar *mean,
        NumericVar *variance,
        uint64 N);
static void final_OnlineAgg_common(OnlineAgg_NumericAggPerGroupState *pergroup,
								   OnlineAgg_NumericAggPerAggState *peragg,
								   double confidence,
								   uint64 N,
								   Datum *p_result,
								   Datum *p_rel_ci);

static NumericAggState *
makeNumericAggStateFromMemoryContext(MemoryContext agg_context, bool calcSumX2)
{
	NumericAggState *state;
	MemoryContext old_context;

	old_context = MemoryContextSwitchTo(agg_context);

	state = (NumericAggState *) palloc0(sizeof(NumericAggState));
	state->calcSumX2 = calcSumX2;
	state->agg_context = agg_context;

	MemoryContextSwitchTo(old_context);

	return state;
}

OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState(Expr *expr) {
	
	if (IsA(expr, OnlineAggSum)) {
		return init_OnlineAgg_NumericAggPerAggState_sum((OnlineAggSum*) expr);
	}
	else if (IsA(expr, OnlineAggCount)) {
		return init_OnlineAgg_NumericAggPerAggState_count((OnlineAggCount*) expr);
	}
	else {
		return NULL;
	}
}

static OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState_sum(OnlineAggSum *sum) {
	OnlineAgg_NumericAggPerAggState_sum *aggstate;
	PGFunction tonumeric = NULL;

	if (sum->arg_typeid != NUMERICOID && 
		NULL == (tonumeric = OnlineAgg_get_tonumeric_func(sum->arg_typeid))) {
		ereport(ERROR, (errmsg("cannot convert type %u to numericvar",
							sum->arg_typeid)));
	}

	aggstate = (OnlineAgg_NumericAggPerAggState_sum *) 
		palloc(sizeof(OnlineAgg_NumericAggPerAggState_sum));
	aggstate->tonumeric = tonumeric;

	aggstate->state.type = T_OnlineAgg_NumericAggPerAggState;
	aggstate->state.expr = sum->arg;
	aggstate->state.expr_state = NULL;
	aggstate->state.initfunc = init_OnlineAgg_NumericAggPerGroupState;
	aggstate->state.transfunc = trans_OnlineAgg_sum;
	aggstate->state.finalfunc = final_OnlineAgg_common;

    aggstate->state.partial_initfunc = init_OnlineAgg_PartialNumericAggPerGroupState;
    aggstate->state.partial_resetfunc = reset_OnlineAgg_PartialNumericAggPerGroupState;
    aggstate->state.partial_transfunc = trans_OnlineAgg_Partial_sum;
    aggstate->state.partial_finalfunc = final_OnlineAgg_Partial_common;

	return (OnlineAgg_NumericAggPerAggState *) aggstate;
}

static OnlineAgg_NumericAggPerAggState *
init_OnlineAgg_NumericAggPerAggState_count(OnlineAggCount *count) {
	OnlineAgg_NumericAggPerAggState_count *aggstate;
	PGFunction tonumeric = NULL;
	
	if (count->scale != NULL) {
		Assert(!count->scale->constisnull);

		if (count->scale->consttype != NUMERICOID && 
			NULL == (tonumeric = OnlineAgg_get_tonumeric_func(count->scale->consttype))) {
			ereport(ERROR, (errmsg("cannot convert type %u to numericvar",
							(unsigned) count->scale->consttype)));
		}
	}

	aggstate = (OnlineAgg_NumericAggPerAggState_count *) 
		palloc(sizeof(OnlineAgg_NumericAggPerAggState_count));
	if (count->scale != NULL) {
		if (tonumeric != NULL)
			aggstate->scale = DatumGetNumeric(DirectFunctionCall1(tonumeric,
												  count->scale->constvalue));
		else
			aggstate->scale = DatumGetNumeric(count->scale->constvalue);
	}
	else {
		aggstate->scale = make_result(&const_one);	
	}

	aggstate->state.type = T_OnlineAgg_NumericAggPerAggState;
	aggstate->state.expr = NULL;
	aggstate->state.expr_state = NULL;
	aggstate->state.initfunc = init_OnlineAgg_NumericAggPerGroupState;
	aggstate->state.transfunc = trans_OnlineAgg_count;
	aggstate->state.finalfunc = final_OnlineAgg_common;

    aggstate->state.partial_initfunc = init_OnlineAgg_PartialNumericAggPerGroupState;
    aggstate->state.partial_resetfunc = reset_OnlineAgg_PartialNumericAggPerGroupState;
    aggstate->state.partial_transfunc = trans_OnlineAgg_Partial_count;
    aggstate->state.partial_finalfunc = final_OnlineAgg_Partial_common;

	return (OnlineAgg_NumericAggPerAggState *) aggstate;
}

static OnlineAgg_NumericAggPerGroupState *
init_OnlineAgg_NumericAggPerGroupState(MemoryContext aggcontext,
									   OnlineAgg_NumericAggPerAggState *peragg) {
	
	return makeNumericAggStateFromMemoryContext(aggcontext, true);
}

static OnlineAgg_PartialNumericAggPerGroupState *
init_OnlineAgg_PartialNumericAggPerGroupState(MemoryContext aggcontext,
                                              OnlineAgg_NumericAggPerAggState *peragg) {
    return makeNumericAggStateFromMemoryContext(aggcontext, false);
}

static void
reset_OnlineAgg_PartialNumericAggPerGroupState(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerAggState *peragg) {
    
    MemoryContext context = partial_pergroup->agg_context;
    memset(partial_pergroup, 0, sizeof(NumericAggState));
    partial_pergroup->agg_context = context;
    partial_pergroup->calcSumX2 = false;
}

static PGFunction 
OnlineAgg_get_tonumeric_func(Oid type) {
	switch (type) {
	case INT2OID:
		return int2_numeric;
	case INT4OID:
		return int4_numeric;
	case INT8OID:
		return int8_numeric;
	case FLOAT4OID:
		return float4_numeric;
	case FLOAT8OID:
		return float8_numeric;
	case NUMERICOID:
		return NULL;
	default:
		return NULL;
	}
}

static void 
trans_OnlineAgg_sum(OnlineAgg_NumericAggPerGroupState *pergroup,
					OnlineAgg_NumericAggPerAggState *peragg,
					Datum value,
					bool isZero,
					const double *inv_prob,
					int nrels) {

	OnlineAgg_NumericAggPerAggState_sum *peragg_sum = 
			(OnlineAgg_NumericAggPerAggState_sum *) peragg;
	Numeric val;

	if (isZero) {
		++pergroup->N;	
		return ;
	}

	if (peragg_sum->tonumeric != NULL) {
		val = DatumGetNumeric(DirectFunctionCall1(peragg_sum->tonumeric, value));
	}
	else {
		val = DatumGetNumeric(value);
	}

	trans_OnlineAgg_common(pergroup, val, inv_prob, nrels);
}

static void
trans_OnlineAgg_Partial_sum(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerAggState *peragg,
    Datum value,
    bool isZero) {
    
	OnlineAgg_NumericAggPerAggState_sum *peragg_sum = 
			(OnlineAgg_NumericAggPerAggState_sum *) peragg;
	Numeric val;
    
	if (isZero) {
		++partial_pergroup->N;	
		return ;
	}

    if (peragg_sum->tonumeric != NULL) {
        val = DatumGetNumeric(DirectFunctionCall1(peragg_sum->tonumeric, value));
    } else {
        val = DatumGetNumeric(value);
    }

    trans_OnlineAgg_Partial_common(partial_pergroup, val);
}

static void 
trans_OnlineAgg_count(OnlineAgg_NumericAggPerGroupState *pergroup,
					  OnlineAgg_NumericAggPerAggState *peragg,
					  Datum value,
					  bool isZero,
					  const double *inv_prob,
					  int nrels) {
	OnlineAgg_NumericAggPerAggState_count *peragg_count = 
				(OnlineAgg_NumericAggPerAggState_count *) peragg;

	if (isZero) {
		++pergroup->N;
		return ;
	}

	trans_OnlineAgg_common(pergroup, peragg_count->scale, inv_prob, nrels);
}

static void
trans_OnlineAgg_Partial_count(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerAggState *peragg,
    Datum value,
    bool isZero) {
    OnlineAgg_NumericAggPerAggState_count *peragg_count = 
				(OnlineAgg_NumericAggPerAggState_count *) peragg;

    if (isZero) {
        ++partial_pergroup->N;
        return ;
    }

    trans_OnlineAgg_Partial_common(partial_pergroup, peragg_count->scale);
}

static void 
trans_OnlineAgg_common(OnlineAgg_NumericAggPerGroupState *pergroup,
					   Numeric value,
					   const double *inv_prob,
					   int nrels) {
	NumericVar var;
	NumericVar tmp;
	int i;

	init_var(&var);
	init_var(&tmp);

	if (NUMERIC_IS_NAN(value)) {
		set_var_from_var(&const_nan, &var);
	}
	else {
		set_var_from_num(value, &var);	
		
		for (i = 1; i <= nrels; ++i) {

			double_to_numericvar(inv_prob[i], &tmp);
			mul_var(&var, &tmp, &var, var.dscale + tmp.dscale);
		}
	}	
	
	do_numericvar_accum(pergroup, &var);

	free_var(&tmp);
	free_var(&var);
}

static void
trans_OnlineAgg_Partial_common(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    Numeric value) {

    NumericVar var;
    init_var(&var);

    if (NUMERIC_IS_NAN(value)) {
        set_var_from_var(&const_nan, &var);
    } else {
        set_var_from_num(value, &var);
    }

    do_numericvar_accum(partial_pergroup, &var);

    free_var(&var);
}

static void
final_OnlineAgg_Partial_common(
    OnlineAgg_PartialNumericAggPerGroupState *partial_pergroup,
    OnlineAgg_NumericAggPerGroupState *pergroup,
    OnlineAgg_NumericAggPerAggState *peragg,
    const double *inv_prob,
    int nrels) {
    
    int i;
    NumericVar var;
    NumericVar tmp;
    init_var(&var);
    init_var(&tmp);
    
    set_var_from_var(&partial_pergroup->sumX, &var);
    for (i = 1; i <= nrels; ++i) {
        double_to_numericvar(inv_prob[i], &tmp);
		mul_var(&var, &tmp, &var, var.dscale + tmp.dscale);
    }

    do_numericvar_accum(pergroup, &var);
    
    free_var(&var);
    free_var(&tmp);
}

static void
calc_mean_variance(
        OnlineAgg_NumericAggPerGroupState *pergroup,
        NumericVar *mean,
        NumericVar *variance,
        uint64 N) {
    NumericAggState *state = pergroup;
    NumericVar tmp;
    int rscale;

    init_var(&tmp);

    if (NULL != mean) {
        uint8_to_numericvar(N, &tmp);
        rscale = select_div_scale(&state->sumX, &tmp);
        div_var(&state->sumX, &tmp, mean, rscale, true); /* mean = X / N */
    }

    
    if (NULL != variance) {
        mul_var(&state->sumX, &state->sumX, &tmp, state->sumX.dscale + tmp.dscale); /* tmp = X^2 */
        int8_to_numericvar(N, variance); /* v = N */
        rscale = select_div_scale(&tmp, variance);
        div_var(&tmp, variance, &tmp, rscale, true); /* tmp = X^2 / N */

        sub_var(&state->sumX2, &tmp, variance); /* v = X2 - X^2 / N */
        int8_to_numericvar(N - 1, &tmp); /* tmp = N - 1 */
        rscale = select_div_scale(variance, &tmp);
        div_var(variance, &tmp, variance, rscale, true); /* v = (X2 - X^2 / N) / (N - 1) */
    }

    free_var(&tmp);
}

void
calc_variance_double(
        OnlineAgg_NumericAggPerGroupState *pergroup,
        double *p_variance,
        uint64 N) {
    NumericVar variance;
    if (N <= 1) {
        *p_variance = XQL_DOUBLE_INF;
    } else {
        init_var(&variance);
        calc_mean_variance(pergroup, NULL, &variance, N);
        *p_variance = numericvar_to_double_no_overflow(&variance);
        free_var(&variance);
    }
}

static void 
final_OnlineAgg_common(OnlineAgg_NumericAggPerGroupState *pergroup,
					   OnlineAgg_NumericAggPerAggState *peragg,
					   double confidence,
					   uint64 N,
					   Datum *p_result,
					   Datum *p_rel_ci) {
	NumericAggState *state = pergroup;
	NumericVar var;
	NumericVar tmp;
	NumericVar tmp2;
	int rscale;
	int sweight;
	
	if (state->NaNcount > 0 || N <= 1) {
		*p_result = NumericGetDatum(make_result(&const_nan));
		*p_rel_ci = NumericGetDatum(make_result(&const_nan));
		return ;
	}
	
	init_var(&var);
	init_var(&tmp);
	
	/* result = X / N 
	 * s = sqrt((X2 - X^2 / N) / (N - 1))
	 * ci = s * (sqrt(2) * erf_inv(confidence)) / sqrt(round) */
    calc_mean_variance(pergroup, &var, &tmp, N); /* var = result, tmp = variance */
	*p_result = NumericGetDatum(make_result(&var));

	if (tmp.ndigits == 0 || tmp.digits[0] == 0) {
		/* result == 0, report zero */
		*p_rel_ci = NumericGetDatum(make_result(&const_zero));
	} else {
        init_var(&tmp2);

		sweight = (tmp.weight + 1) * DEC_DIGITS / 2 - 1;
		rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
		rscale = Max(rscale, tmp.dscale);
		rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
		rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);
		sqrt_var(&tmp, &tmp, rscale); /* tmp == sqrt((X2 - X^2 / N) / (N - 1)) (s) */

		/* s * (sqrt(2) * erf_inv(confidence)*/
		double_to_numericvar(xql_sqrt(2.0) * xql_erf_inv(confidence), &tmp2); /* tmp2 = sqrt(2) * erf_inv(confidence) */
		mul_var(&tmp2, &tmp, &tmp, var.dscale + tmp.dscale); /* tmp = s * (sqrt(2) * erf_inf(confidence))*/

		double_to_numericvar(xql_sqrt((double) N), &tmp2); /* tmp2 = sqrt(N) */
		rscale = select_div_scale(&tmp, &tmp2);
		div_var(&tmp, &tmp2, &tmp, rscale, true); /* tmp = ci */
		
		rscale = select_div_scale(&tmp, &var);
		div_var(&tmp, &var, &tmp2, 6, true); /* tmp2 = ci / result */

		*p_rel_ci = NumericGetDatum(make_result(&tmp2));

		free_var(&tmp2);
	}

	free_var(&tmp);
	free_var(&var);
}


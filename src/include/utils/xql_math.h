/*-----------------------------------------------------------
 *
 * xql_math.h
 *     Math functions for online aggregation.
 *
 * Copyright (c) 2015-2017, InitialD Lab
 *
 * src/include/utils/xql_math.h
 *
 *-----------------------------------------------------------
 */

#ifndef _XQL_MATH_H_
#define _XQL_MATH_H_

#include "postgres.h"
#include <math.h>

#ifndef PG_USE_INLINE
extern double xql_erf_inv(double p);
extern double xql_sqrt(double p);
extern uint64 xql_randint(uint64 high);
extern void xql_drand_init(void);
extern double xql_drand(void);
#endif

#ifdef INFINITY
#	define XQL_INF (INFINITY)
#	define XQL_DOUBLE_INF ((double) INFINITY)
#else
#	define XQL_INF (-log(0.0))
#	define XQL_DOUBLE_INF ((double)(-log(0.0)))
#endif

#ifdef isinf
#	define xql_isinf(arg) isinf(arg)
#else
#	define xql_isinf(arg) ((arg) == XQL_INF)
#endif

#ifdef NAN
#	define XQL_NAN (NAN)
#else
#	define XQL_NAN (sqrt(-1.0))
#endif

#ifdef isnan
#	define xql_isnan(arg) isnan(arg)
#else

#ifdef PG_USE_INLINE
inline
#endif
static bool xql_isnan_impl(double x) { return x != x; };

#	define xql_isnan(arg) xql_isnan_impl(arg)
#endif


#if defined(PG_USE_INLINE) || defined(XQL_MATH_INCLUDE_DEFINITIONS)

/**
 * erf_inv is adapted from the implementation of apache commons math3
 */
STATIC_IF_INLINE double xql_erf_inv(double x) {
    double w = - log((1.0 - x) * (1.0 + x));
    double p;

    if (w < 6.25) {
		w -= 3.125;
		p =  -3.6444120640178196996e-21;
		p =   -1.685059138182016589e-19 + p * w;
		p =   1.2858480715256400167e-18 + p * w;
		p =    1.115787767802518096e-17 + p * w;
		p =   -1.333171662854620906e-16 + p * w;
		p =   2.0972767875968561637e-17 + p * w;
		p =   6.6376381343583238325e-15 + p * w;
		p =  -4.0545662729752068639e-14 + p * w;
		p =  -8.1519341976054721522e-14 + p * w;
		p =   2.6335093153082322977e-12 + p * w;
		p =  -1.2975133253453532498e-11 + p * w;
		p =  -5.4154120542946279317e-11 + p * w;
		p =    1.051212273321532285e-09 + p * w;
		p =  -4.1126339803469836976e-09 + p * w;
		p =  -2.9070369957882005086e-08 + p * w;
		p =   4.2347877827932403518e-07 + p * w;
		p =  -1.3654692000834678645e-06 + p * w;
		p =  -1.3882523362786468719e-05 + p * w;
		p =    0.0001867342080340571352 + p * w;
		p =  -0.00074070253416626697512 + p * w;
		p =   -0.0060336708714301490533 + p * w;
		p =      0.24015818242558961693 + p * w;
		p =       1.6536545626831027356 + p * w;
	} else if (w < 16.0) {
		w = sqrt(w) - 3.25;
		p =   2.2137376921775787049e-09;
		p =   9.0756561938885390979e-08 + p * w;
		p =  -2.7517406297064545428e-07 + p * w;
		p =   1.8239629214389227755e-08 + p * w;
		p =   1.5027403968909827627e-06 + p * w;
		p =   -4.013867526981545969e-06 + p * w;
		p =   2.9234449089955446044e-06 + p * w;
		p =   1.2475304481671778723e-05 + p * w;
		p =  -4.7318229009055733981e-05 + p * w;
		p =   6.8284851459573175448e-05 + p * w;
		p =   2.4031110387097893999e-05 + p * w;
		p =   -0.0003550375203628474796 + p * w;
		p =   0.00095328937973738049703 + p * w;
		p =   -0.0016882755560235047313 + p * w;
		p =    0.0024914420961078508066 + p * w;
		p =   -0.0037512085075692412107 + p * w;
		p =     0.005370914553590063617 + p * w;
		p =       1.0052589676941592334 + p * w;
		p =       3.0838856104922207635 + p * w;
	} else if (!xql_isinf(w)) {
		w = sqrt(w) - 5.0;
		p =  -2.7109920616438573243e-11;
		p =  -2.5556418169965252055e-10 + p * w;
		p =   1.5076572693500548083e-09 + p * w;
		p =  -3.7894654401267369937e-09 + p * w;
		p =   7.6157012080783393804e-09 + p * w;
		p =  -1.4960026627149240478e-08 + p * w;
		p =   2.9147953450901080826e-08 + p * w;
		p =  -6.7711997758452339498e-08 + p * w;
		p =   2.2900482228026654717e-07 + p * w;
		p =  -9.9298272942317002539e-07 + p * w;
		p =   4.5260625972231537039e-06 + p * w;
		p =  -1.9681778105531670567e-05 + p * w;
		p =   7.5995277030017761139e-05 + p * w;
		p =  -0.00021503011930044477347 + p * w;
		p =  -0.00013871931833623122026 + p * w;
		p =       1.0103004648645343977 + p * w;
		p =       4.8499064014085844221 + p * w;
	} else {
		p = XQL_DOUBLE_INF;
	}

	return p * x;
}

STATIC_IF_INLINE double xql_sqrt(double p) {
	return sqrt(p);
}

STATIC_IF_INLINE uint64
xql_randint(uint64 high) {
	uint64	mask;
	uint64	result;
	
	if (high == 0) return 0;

	mask = high;
	mask |= mask >> 1;
	mask |= mask >> 2;
	mask |= mask >> 4;
	mask |= mask >> 8;
	mask |= mask >> 16;
	mask |= mask >> 32;

	if (mask <= UINT64CONST(0x7FFFFFFF)) {
		if ((high + 1) & high) {
			/* high != power of 2 - 1 */
			while ((result = (((uint64) random()) & mask)) > high) ;
		}
		else {
			/* high = power of 2 - 1*/
			result = ((uint64) random()) & mask;
		}
	}
	else if (mask <= UINT64CONST(0x3FFFFFFFFFFFFFFF)) {
		if ((high + 1) & high) {
			/* high != power of 2 - 1 */
			while ((result = ((((uint64) random()) << 31) | ((uint64) random())) & mask) > high) ;
		}
		else {
			/* high = power of 2 - 1*/
			result = ((((uint64) random()) << 31) | ((uint64) random())) & mask;
		}
	} else {
		if ((high + 1) & high) {
			/* high != power of 2 - 1 */
			while ((result = ((((uint64) random()) << 62) | (((uint64) random()) << 31) | ((uint64) random())) & mask) > high) ;
		}
		else {
			/* high = power of 2 - 1*/
			result = ((((uint64) random()) << 62) | (((uint64) random()) << 31) | ((uint64) random())) & mask;
		}
	}

	return result;
}

#endif

#endif

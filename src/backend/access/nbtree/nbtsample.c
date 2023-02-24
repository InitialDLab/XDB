/*-----------------------------------------------------------------
 * nbtsample.c
 *    Implementation of sampling from btree
 *
 * Copyright (c) 2015-2017, InitialD Lab
 *
 * IDENTIFICATION
 *    src/backend/access/nbtree/nbtree.c
 *
 *-----------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "catalog/index.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/xql_math.h"

/*
typedef struct BTSampleStateData_debug {
	Relation btss_index;

	bool btss_qual_ok;
	BTSampleStrategy btss_strategy;

	ScanKeyData btss_lowkey_data;

	ScanKeyData btss_highkey_data;

	bool btss_want_itup;
	IndexTuple btss_sample_itup;
	TupleDesc btss_sample_itupdesc;

	bool btss_sample_offset_count_valid;
	uint64 btss_sample_offset;
	uint64 btss_sample_count;
	ItemPointerData btss_sample_tid;
} BTSampleStateData_debug;

typedef BTSampleStateData_debug *_btss_debug_t;

static _btss_debug_t _btss_debug __attribute__((unused)) = NULL; */

static int _bt_sample_opposite_strategy_number_data[BTMaxStrategyNumber + 1] 
__attribute__((unused)) = {
    0, 4, 5, 3, 1, 2
};
#define _bt_sample_opposite_strategy_number(strat)  \
    _bt_sample_opposite_strategy_number_data[strat]

static void _bt_sample_check_static_assertions(void) __attribute__((unused));
static void _bt_sample_check_static_assertions(void) {
	StaticAssertStmt(BTLessStrategyNumber == 1, "BTLessStrategyNumber != 1");
	StaticAssertStmt(BTLessEqualStrategyNumber == 2, "BTLessEqualStrategyNumber != 2");
	StaticAssertStmt(BTEqualStrategyNumber == 3, "BTEqualStrategyNumber != 3");
	StaticAssertStmt(BTGreaterEqualStrategyNumber == 4, "BTGreaterStrategyNumber != 4");
	StaticAssertStmt(BTGreaterStrategyNumber == 5, "BTGreaterEqualStrategyNumber != 5");
	StaticAssertStmt(BTMaxStrategyNumber == 5, "BTMaxStrategyNumber != 5");

	StaticAssertStmt(BTSAMPLE_LOWKEY == 1, "BTSAMPLE_LOWKEY != 1");
	StaticAssertStmt(BTSAMPLE_HIGHKEY == 2, "BTSAMPLE_HIGHKEY != 2");
	StaticAssertStmt(BTSAMPLE_RANGE == (BTSAMPLE_LOWKEY | BTSAMPLE_HIGHKEY),
					 "BTSAMPLE_RANGE != BTSAMPLE_LOWKEY | BTSAMPLE_HIGHKEY");
	StaticAssertStmt(BTSAMPLE_EQUAL == 4, "BTSAMPLE_EQUAL != 4");

	/*
	StaticAssertStmt(((BTSampleState)NULL)->btss_keys == ((BTSampleState)NULL)->btss_lowkey,
					 "btss_keys and btss_lowkey have different offset");
	StaticAssertStmt(((BTSampleState)NULL)->btss_lowkey == &((BTSampleState)NULL)->btss_lowkey_data,
					 "btss_lowkey and btss_lowkey_data have different offset");
	StaticAssertStmt(&((BTSampleState)NULL)->btss_lowkey[1] == ((BTSampleState)NULL)->btss_highkey,
					 "btss_lowkey[1] and btss_highkey have different offset");
	StaticAssertStmt(((BTSampleState)NULL)->btss_highkey == &((BTSampleState)NULL)->btss_highkey_data,
					 "btss_highkey and btss_highkey_data have differnt offset");
					 */
}

static void _bt_sample_search_key_to_insertion_key(Relation index, ScanKey key);
static int32 _bt_sample_compare(Relation index, ScanKey key, Page page, OffsetNumber off);
#ifndef PG_USE_INLINE
static int32 _bt_sample_compare_one(TupleDesc itupdesc, ScanKey key, IndexTuple itup);
#endif
static OffsetNumber _bt_sample_binsrch(Relation index, Buffer buf, ScanKey key,
									   int32 allow_equal);
static void _bt_sample_impl_get_keys(BTSampleState state,
                                     ScanKey *lowkey,
                                     int32 *lowkey_allow_equal,
                                     ScanKey *highkey,
                                     int32 *highkey_allow_equal);
static bool _bt_sample_impl(BTSampleState state);

/*
 * _bt_sample_randint()
 *
 * returns a random unsigned 64-bit number in range [0 .. high].
 */
#define _bt_sample_randint xql_randint

/* these macros assume that the following routines name BTSampleState state */
#define BTSS_KEYS (state->btss_keys)
#define BTSS_LOWKEY (&state->btss_keys[0])
#define BTSS_LOWKEY_DATA (state->btss_keys[0])
#define BTSS_HIGHKEY (&state->btss_keys[1])
#define BTSS_HIGHKEY_DATA (state->btss_keys[1])

BTSampleState _bt_init_sample_state(Relation index, bool want_itup, bool sample_all_on_leaf) {
	BTSampleState state = (BTSampleState) palloc(sizeof(BTSampleStateData));

	state->btss_index = index;
	state->btss_qual_ok = false;
	state->btss_strategy = BTSAMPLE_NOKEY;
	state->btss_want_itup = want_itup;
	state->btss_sample_itup = NULL;
	state->btss_sample_itupdesc = RelationGetDescr(index);
    state->btss_inverse_probability = 1.0;
	ItemPointerSetInvalid(&state->btss_sample_tid);

    if ((state->btss_sample_all_on_leaf = sample_all_on_leaf)) {
        state->btss_all_tid_on_leaf = (ItemPointerData *) palloc(sizeof(ItemPointerData) * MaxIndexTuplesPerPage);
        state->btss_curpos_on_leaf = 0;
        state->btss_nitems_on_leaf = 0;
    }

	return state;
}

BTSampleState _bt_prepare_sample_state(Relation index, int nkeys, ScanKey keys,
									   bool want_itup, bool sample_all_on_leaf) {
	BTSampleState state;

	state = _bt_init_sample_state(index, want_itup, sample_all_on_leaf);
	_bt_sample_set_key(state, nkeys, keys);

	return state;
}

void _bt_destroy_sample_state(BTSampleState state) {
	if (state->btss_sample_itup != NULL)
		pfree(state->btss_sample_itup);
    if (state->btss_sample_all_on_leaf)
        pfree(state->btss_all_tid_on_leaf);
	pfree(state);
}

static int _bt_sample_strategy_transition[BTMaxStrategyNumber+1][BTSAMPLE_STRATEGY_NUM] = {
	{},
	{BTSAMPLE_HIGHKEY,	BTSAMPLE_RANGE, BTSAMPLE_HIGHKEY, BTSAMPLE_RANGE, BTSAMPLE_EQUAL},
	{BTSAMPLE_HIGHKEY,	BTSAMPLE_RANGE, BTSAMPLE_HIGHKEY, BTSAMPLE_RANGE, BTSAMPLE_EQUAL},
	{BTSAMPLE_EQUAL,	BTSAMPLE_EQUAL, BTSAMPLE_EQUAL,	  BTSAMPLE_EQUAL, BTSAMPLE_EQUAL},
	{BTSAMPLE_LOWKEY,	BTSAMPLE_LOWKEY, BTSAMPLE_RANGE,  BTSAMPLE_RANGE, BTSAMPLE_EQUAL},
	{BTSAMPLE_LOWKEY,	BTSAMPLE_LOWKEY, BTSAMPLE_RANGE,  BTSAMPLE_RANGE, BTSAMPLE_EQUAL},
};

static void _bt_sample_search_key_to_insertion_key(Relation index, ScanKey key) {
	Assert(key->sk_attno == 1);
	if (!(key->sk_flags & SK_ROW_HEADER)) {
		/* need to replace the sk_func with the appropriate
		 * btree comparison function */

		if (key->sk_subtype == index->rd_opcintype[0] ||
			key->sk_subtype == InvalidOid) {

			FmgrInfo *procInfo;
			procInfo = index_getprocinfo(index, 1, BTORDER_PROC);
			fmgr_info_copy(&key->sk_func, procInfo, CurrentMemoryContext);

		} else {
			RegProcedure cmp_proc;

			cmp_proc = get_opfamily_proc(index->rd_opfamily[0],
										 index->rd_opcintype[0],
										 key->sk_subtype,
										 BTORDER_PROC);
			if (!RegProcedureIsValid(cmp_proc))
				elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
					 BTORDER_PROC, index->rd_opcintype[0], key->sk_subtype,
					 1, RelationGetRelationName(index));
			fmgr_info(cmp_proc, &key->sk_func);
		}
	}
	/* else nothing to do */
}

void _bt_sample_set_key(BTSampleState state, int nkeys, ScanKey keys) {
	Relation	index = state->btss_index;
	int16		*indoption = index->rd_indoption;
#ifdef USE_ASSERT_CHECKING
	int			natts = RelationGetNumberOfAttributes(index);
#endif
	int			i;
	ScanKeyData	tmpKey;
	bool		test_result1, test_result2;

	state->btss_qual_ok = true;
	state->btss_strategy = BTSAMPLE_NOKEY;
	/* state->btss_sample_offset_count_valid = false; */
	ItemPointerSetInvalid(&state->btss_sample_tid);

	for (i = 0; i < nkeys; ++i) {
		/* need to copy the keys, ?? what about row keys */
		tmpKey = keys[i];

		if (tmpKey.sk_attno != 1) {
			/* cannot sample with keys starting from 2 or above */
			state->btss_qual_ok = false;
			return ;
		}

		if (!_bt_fix_scankey_strategy(&tmpKey, indoption)) {
			state->btss_qual_ok = false;
			return ;
		}

		/* have to check if the row columns are in the index column order.
		 * If not, we cannot decide a continuous range to sample from. */
		if (tmpKey.sk_flags & SK_ROW_HEADER) {
			ScanKey subkey = (ScanKey) DatumGetPointer(tmpKey.sk_argument);
			int j = 1;

			for (;; ++j, ++subkey) {
				Assert(j <= natts);
				Assert(subkey->sk_flags & SK_ROW_MEMBER);
				if (subkey->sk_attno != j) {
					state->btss_qual_ok = false;
					return ;
				}
				/* NULL is ok */
				if (subkey->sk_flags & SK_ROW_END) {
					break; /* done checking */
				}
			}
		}

		/**
		 * op0 is > or >=, op1 is < or <=
		 *
		 * apply the key if
		 *					x op0 l		x op1 h		both				x = l
		 * x op t	NOKEY	LOWKEY		HIGHKEY		RANGE				EQUAL
		 * <  (1)	true	l op t		t op1 h		l op t && t op1 h	l op t
		 * <= (2)	true	t op0 l		t op1 h		t op0 l && t op1 h	l op t
		 * =  (3)	true	t op0 l		t op1 h		t op0 l && t op1 h	l op t
		 * >= (4)	true	t op0 l		t op1 h		t op0 l && t op1 h	l op t
		 * >  (5)	true	t op0 l		h op t		t op0 l && h op t	l op t
		 *
		 * otherwise	0		1			2			3				4
		 * x op tmp		NOKEY	LOWKEY		HIGHKEY		RANGE			EQUAL
		 * <  (1)				empty		keep		empty keep		empty
		 * <= (2)				empty		keep		empty keep		empty
		 * =  (3)				empty		empty		empty empty		empty
		 * >= (4)				keep		empty		keep empty		empty
		 * >  (5)				keep		empty		keep empty		empty
		 *
		 * empty: the range is empty set
		 * keep: the key is less strict than the current range; just continue
		 * empty keep / keep empty: if the empty expression evaluates to false,
		 *	the range is empty; otherwise, if the keep expression evaluates to
		 *	false, the current range is more strict than tmpKey
		 *
		 */
#define _bt_sample_check_keys(op, key0, key1, res)	\
		do {	\
			if (!_bt_compare_scankey_args_noscandesc(index,	\
					(op), (key0), (key1), res)){	\
				ereport(ERROR, (errmsg("btsample cannot compare keys")));	\
			}	\
		} while(0)

		/* lowkey || range || equal */
		if (state->btss_strategy & 0x5) {
			if ((((state->btss_strategy & 0x5) << 1) - tmpKey.sk_strategy) >= 1) {
				/* l op t*/
				_bt_sample_check_keys(&tmpKey, BTSS_LOWKEY, &tmpKey, &test_result1);
			} else {
				/* t op0 l */
				_bt_sample_check_keys(BTSS_LOWKEY, &tmpKey, BTSS_LOWKEY, &test_result1);
			}

			if (!test_result1) {
				if (!((state->btss_strategy & 0x1) & (tmpKey.sk_strategy >= 4))) {
					/* empty */
					state->btss_qual_ok = true;
					//state->btss_sample_offset_count_valid = true;
					//state->btss_sample_offset = state->btss_sample_count = 0;
					return ;
				}
			}
		} else test_result1 = true;

		/* highkey || range */
		if (state->btss_strategy & 0x2) {
			if (tmpKey.sk_strategy == 5) {
				/* h op t */
				_bt_sample_check_keys(&tmpKey, BTSS_HIGHKEY, &tmpKey, &test_result2);
			} else {
				/* t op1 h */
				_bt_sample_check_keys(BTSS_HIGHKEY, &tmpKey, BTSS_HIGHKEY, &test_result2);
			}

			if (!test_result2) {
				if (tmpKey.sk_strategy >= 3) {
					/* empty */
					state->btss_qual_ok = true;
					//state->btss_sample_offset_count_valid = true;
					//state->btss_sample_offset = state->btss_sample_count = 0;
					return ;
				}
			}
		} else test_result2 = true;

		if (test_result1 & test_result2) {
			BTSS_KEYS[(tmpKey.sk_strategy >= 3) ? 0 : 1] = tmpKey;
			state->btss_strategy =
				_bt_sample_strategy_transition[tmpKey.sk_strategy][state->btss_strategy];
		}
		else if (test_result1 ^ test_result2) {
			continue;
		}
		else {
			state->btss_qual_ok = true;
			//state->btss_sample_offset_count_valid = true;
			//state->btss_sample_offset = state->btss_sample_count = 0;
			return ;
		}
	}

	/* convert the search scankeys to insertion scankeys */
	if (state->btss_strategy & 0x5)
		_bt_sample_search_key_to_insertion_key(index, BTSS_LOWKEY);
	if (state->btss_strategy & 0x2)
		_bt_sample_search_key_to_insertion_key(index, BTSS_HIGHKEY);
}

static
#ifdef PG_USE_INLINE
inline
#endif
int32 _bt_sample_compare_one(TupleDesc itupdesc, ScanKey key, IndexTuple itup)  {
	Datum		datum;
	bool		is_null;
	int32		result;

	datum = index_getattr(itup, key->sk_attno, itupdesc, &is_null);

	if (key->sk_flags & SK_ISNULL) {
		if (is_null)
			result = 0;
		else if (key->sk_flags & SK_BT_NULLS_FIRST)
			result = -1;
		else
			result = 1;
	}
	else if (is_null) {
		if (key->sk_flags & SK_BT_NULLS_FIRST)
			result = 1;
		else
			result = -1;
	}
	else {
		result = DatumGetInt32(FunctionCall2Coll(&key->sk_func,
												 key->sk_collation,
												 datum,
												 key->sk_argument));

		if (!(key->sk_flags & SK_BT_DESC))
			result = -result;
	}

	return result;
} 
static int32
_bt_sample_compare(Relation index, ScanKey key,
				   Page page, OffsetNumber off) {
	BTPageOpaque	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	TupleDesc		itupdesc = RelationGetDescr(index);
	IndexTuple		itup;
	ItemId			itemid;
	int32			result;

	/* the first key in an internal page is always assumed to be -oo */
	if (!P_ISLEAF(opaque) && off == P_FIRSTDATAKEY(opaque)) {
		return 1;
	}

	itemid = PageGetItemId(page, off);
	itup = (IndexTuple) PageGetItem(page, itemid);

	if (key->sk_flags & SK_ROW_HEADER) {
		ScanKey		subkey = (ScanKey) key->sk_argument;

		for ( ;; ++subkey) {

			result = _bt_sample_compare_one(itupdesc, subkey, itup);
			if (result != 0) {
				break;
			}
			if (subkey->sk_flags & SK_ROW_END) {
				break;
			}
		}
	}
	else {
		result = _bt_sample_compare_one(itupdesc, key, itup);
	}

	return result;
}

/**
 *
 * Find the first value that is >= key (if allow_equal == 1)
 * or > key (if allow_equal == 0). The lowest index is 
 * returned when the page is empty.
 *
 */
static OffsetNumber
_bt_sample_binsrch(Relation index, Buffer buf,
				   ScanKey key, int32 allow_equal) {
	OffsetNumber	low,
					mid,
					high;
	int32			result;
	Page			page;
	BTPageOpaque	opaque;

	page = BufferGetPage(buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	low = P_FIRSTDATAKEY(opaque);
	high = PageGetMaxOffsetNumber(page);

	if (low > high) {
        /* The page is empty. */
		return low;
	}

	++high;
	while (low < high) {
		mid = low + ((high - low) >> 1);

		result = _bt_sample_compare(index, key, page, mid);
		if (result >= allow_equal) {
			low = mid + 1;
		}
		else {
			high = mid;
		}
	}

    if (!P_ISLEAF(opaque)) {
        Assert(low > P_FIRSTDATAKEY(opaque));
        low = OffsetNumberPrev(low);
    }

	return low;
}

bool _bt_sample(BTSampleState state) {
    bool        result;

	if (!state->btss_qual_ok) return false;
    result = _bt_sample_impl(state);

	return result;
}

/* 
 * lowkey <=/< x <=/< highkey 
 * returns x \in [lowkey', highkey')
 *
 * */
static void
_bt_sample_impl_get_keys(BTSampleState state,
                         ScanKey *lowkey,
                         int32 *lowkey_allow_equal,
                         ScanKey *highkey,
                         int32 *highkey_allow_equal) {
    switch (state->btss_strategy) {
    case BTSAMPLE_NOKEY:
        *lowkey = NULL;
        *lowkey_allow_equal = 0;
        *highkey = NULL;
        *highkey_allow_equal = 0;
        break;

    case BTSAMPLE_LOWKEY:
        *lowkey = BTSS_LOWKEY;
	    *lowkey_allow_equal = (BTSS_LOWKEY->sk_strategy == BTGreaterEqualStrategyNumber) ? 1 : 0;
        *highkey = NULL;
        *highkey_allow_equal = 0;
        break;

    case BTSAMPLE_HIGHKEY:
        *lowkey = NULL;
        *lowkey_allow_equal = 0;
        *highkey = BTSS_HIGHKEY;
        *highkey_allow_equal = (BTSS_HIGHKEY->sk_strategy == BTLessStrategyNumber) ? 1 : 0;
        break;

    case BTSAMPLE_RANGE:
        *lowkey = BTSS_LOWKEY;
        *lowkey_allow_equal = (BTSS_LOWKEY->sk_strategy == BTGreaterEqualStrategyNumber) ? 1 : 0;
        *highkey = BTSS_HIGHKEY;
        *highkey_allow_equal = (BTSS_HIGHKEY->sk_strategy == BTLessStrategyNumber) ? 1 : 0;
        break;

    case BTSAMPLE_EQUAL:
        *lowkey = BTSS_LOWKEY;
        *lowkey_allow_equal = 1;
        *highkey = BTSS_LOWKEY;
        *highkey_allow_equal = 0;
        break;

    default:
        ereport(ERROR, (errmsg("unknown btsample strategy %d", state->btss_strategy)));
    }
}

static bool
_bt_sample_impl(BTSampleState state) {
    Relation        index = state->btss_index;
    Buffer          buf;
    Page            page;
    BTPageOpaque    opaque;
    OffsetNumber    low,
                    high,
                    rand_ind;
    ItemId          itemid;
    IndexTuple      itup;
    BlockNumber     blkno;
    ScanKey         lowkey,
                    highkey;
    int32           lowkey_allow_equal,
                    highkey_allow_equal;
    
    state->btss_inverse_probability = 1.0;
    _bt_sample_impl_get_keys(state, &lowkey, &lowkey_allow_equal, &highkey, &highkey_allow_equal);

    buf = _bt_getroot(state->btss_index, BT_READ);
    if (!BufferIsValid(buf)) return false;

    for (;;) {
        page = BufferGetPage(buf);
        opaque = (BTPageOpaque) PageGetSpecialPointer(page);

        if (NULL != lowkey) {
            low = _bt_sample_binsrch(index, buf, lowkey, lowkey_allow_equal);
        } else {
            low = P_FIRSTDATAKEY(opaque);
        }
        
        if (NULL != highkey) {
            high = _bt_sample_binsrch(index, buf, highkey, highkey_allow_equal);
            if (!P_ISLEAF(opaque)) ++high; /* the first out of range */
        } else {
            high = PageGetMaxOffsetNumber(page);
            ++high;
        }

        if (low >= high) {
            ItemPointerSetInvalid(&state->btss_sample_tid);
            break;
        }
        
        if (!P_ISLEAF(opaque) || !state->btss_sample_all_on_leaf) {
            state->btss_inverse_probability *= high - low;
            rand_ind = _bt_sample_randint((high - low - 1)) + low;
            itemid = PageGetItemId(page, rand_ind);
            itup = (IndexTuple) PageGetItem(page, itemid);

            if (P_ISLEAF(opaque)) {
                state->btss_sample_tid = itup->t_tid;
                if (state->btss_want_itup) {
                    if (state->btss_sample_itup) {
                        pfree(state->btss_sample_itup);
                    }
                    state->btss_sample_itup = CopyIndexTuple(itup);
                }
                break;
            }
            else {
                blkno = ItemPointerGetBlockNumber(&(itup->t_tid));
                buf = _bt_relandgetbuf(index, buf, blkno, BT_READ);
            }
        } else {
            state->btss_nitems_on_leaf = 0; 
            for (; low < high; ++low) {
                itemid = PageGetItemId(page, low);
                itup = (IndexTuple) PageGetItem(page, itemid);
                state->btss_all_tid_on_leaf[state->btss_nitems_on_leaf++] = itup->t_tid;
            }
            state->btss_curpos_on_leaf = 0;
            ItemPointerSetInvalid(&state->btss_sample_tid); /* return this at the end */
            break;
        }
    }
    
    _bt_relbuf(index, buf);
    return true;
}

extern ItemPointer _bt_sample_get_next_tid(BTSampleState state) {
    if (state->btss_curpos_on_leaf < state->btss_nitems_on_leaf) {
        ++state->btss_curpos_on_leaf;
        return &state->btss_all_tid_on_leaf[state->btss_curpos_on_leaf - 1];
    } else {
        return &state->btss_sample_tid; /* which is invalid */
    }
}


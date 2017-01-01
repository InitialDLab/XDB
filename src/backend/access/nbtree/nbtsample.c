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

static int _bt_sample_opposite_strategy_number_data[BTMaxStrategyNumber + 1] = {
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

typedef struct BTValidateCountStackData {
	struct BTValidateCountStackData *btvcs_parent;
	int32 btvcs_ind; /* The index of the item being checked. */
	uint32 btvcs_n_items;
	struct _bt_vc_itup_t{
		ItemPointerData		t_tid;
		uint64				t_sum;
	} btvcs_tuple[0];
} BTValidateCountStackData;

typedef BTValidateCountStackData *BTValidateCountStack;

static BTValidateCountStack _bt_alloc_btvalidatecountstack(BTValidateCountStack parent,
														   uint32 n_items);
static BTValidateCountStack _bt_free_vcstack_and_get_parent(BTValidateCountStack stack);
static void _bt_sample_search_key_to_insertion_key(Relation index, ScanKey key);
static uint64 _bt_sample_count_all(Relation index);
static uint64 _bt_sample_count_left(Relation index, ScanKey key);
static uint64 _bt_sample_count_right(Relation index, ScanKey key);
static int32 _bt_sample_compare(Relation index, ScanKey key, Page page, OffsetNumber off);
static OffsetNumber _bt_sample_binsrch(Relation index, Buffer buf, ScanKey key, 
									   int32 continue_val);
static uint64 _bt_sample_count_right_impl(Relation index, ScanKey key, Buffer buf);
static uint64 _bt_sample_count_range(BTSampleState state) __attribute__((unused));
static bool _bt_sample_retrieve_nth_from_leftmost(BTSampleState state, uint64 n);
static bool _bt_sample_retrieve_nth_from_leftmost_impl(BTSampleState state, uint64 n,
													  Buffer buf);

/*
 * _bt_sample_randint()
 *
 * returns a random unsigned 64-bit number in range [0 .. high].
 */
#define _bt_sample_randint xql_randint

static BTValidateCountStack
_bt_alloc_btvalidatecountstack(BTValidateCountStack parent,
							   uint32 n_items) {
	BTValidateCountStack stack = (BTValidateCountStack) 
		palloc(sizeof(BTValidateCountStackData) + sizeof(struct _bt_vc_itup_t) * n_items);
	stack->btvcs_parent = parent;
	stack->btvcs_ind = -1;
	stack->btvcs_n_items = n_items;
	return stack;
}

static BTValidateCountStack
_bt_free_vcstack_and_get_parent(BTValidateCountStack stack) {
	BTValidateCountStack parent = stack->btvcs_parent;
	pfree(stack);
	return parent;
}

#define _bt_validate_report(...)	\
	ereport(LOG, (errmsg(__VA_ARGS__), errhidestmt(true)))
#define _bt_validate_assert(expr, ...) \
	do {	\
		if (!(expr)) {	\
			_bt_validate_report(__VA_ARGS__);	\
		}	\
	} while (0)

void _bt_validate_tuple_counts(Relation index) {
	BTValidateCountStack stack = NULL;
	Buffer  buf;

	_bt_validate_report("_bt_validate_tuple_count start");
	
	buf = _bt_getroot(index, BT_READ);

	while (BufferIsValid(buf)) {
		Page				page;
		BTPageOpaque		opaque;
		OffsetNumber		low,
							high;
		uint32				n_items;
		uint64				sum;
		BlockNumber			blkno;
		ItemId				itemid;
		IndexTuple			itup;
		
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		blkno = BufferGetBlockNumber(buf);
	
		if (stack != NULL) {
			_bt_validate_report("validating blk %u, expected sum = %lu", blkno,
					stack->btvcs_tuple[stack->btvcs_ind].t_sum);
		}
		else {
			_bt_validate_report("validating root blk %u", blkno);
		}

		if (stack != NULL) {
			_bt_validate_assert(
				stack->btvcs_tuple[stack->btvcs_ind].t_sum == opaque->btpo_sum_tuple_count,
				"blk %u, expected sum = %lu, sum = %lu", blkno,
				stack->btvcs_tuple[stack->btvcs_ind].t_sum, opaque->btpo_sum_tuple_count);
		}

		low = P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);
		n_items = high - low + 1;

		_bt_validate_report("blk %u has %u items, low = %u", blkno, n_items, (unsigned) low);

		if (P_ISLEAF(opaque)) {
			sum = n_items;
		}
		else { /* non-leaf page */
			uint32 i;

			stack = _bt_alloc_btvalidatecountstack(stack, n_items);
			sum = 0;
			for (i = 0; i < n_items; ++i) {
				itemid = PageGetItemId(page, low + i);
				itup = (IndexTuple) PageGetItem(page, itemid);
				
				_bt_validate_assert(IndexTupleHasTupleCount(itup),
					"blk %u, item %u, does not have tuple count", blkno, i);

				stack->btvcs_tuple[i].t_sum = index_tuple_get_count(itup);
				stack->btvcs_tuple[i].t_tid = itup->t_tid;
				sum += stack->btvcs_tuple[i].t_sum;

				_bt_validate_report("blk %u, ind %u, count = %lu", blkno, i, 
						stack->btvcs_tuple[i].t_sum);
			}
		}

		_bt_validate_report("blk %u, sum = %lu", blkno, sum);
		_bt_validate_assert(sum == opaque->btpo_sum_tuple_count,
				"blk %u, sum = %lu, computed sum = %lu", blkno,
				opaque->btpo_sum_tuple_count, sum);

		_bt_relbuf(index, buf);

		if (stack == NULL) break;
		for (;;) {
			if (stack->btvcs_ind < stack->btvcs_n_items) {
				++stack->btvcs_ind;
				blkno = ItemPointerGetBlockNumber(&(stack->btvcs_tuple[stack->btvcs_ind].t_tid));
				buf = _bt_getbuf(index, blkno, BT_READ);
				break;
			}
			else {
				stack = _bt_free_vcstack_and_get_parent(stack);
				if (stack == NULL) {
					buf = InvalidBuffer;
					break;
				}
			}
		}
	}

	_bt_validate_report("_bt_validate_tuple_count done");
}

/* these macros assume that the following routines name BTSampleState state */
#define BTSS_KEYS (state->btss_keys)
#define BTSS_LOWKEY (&state->btss_keys[0])
#define BTSS_LOWKEY_DATA (state->btss_keys[0])
#define BTSS_HIGHKEY (&state->btss_keys[1])
#define BTSS_HIGHKEY_DATA (state->btss_keys[1])

BTSampleState _bt_init_sample_state(Relation index, bool want_itup) {
	BTSampleState state = (BTSampleState) palloc(sizeof(BTSampleStateData));

	state->btss_index = index;
	state->btss_qual_ok = false;
	state->btss_strategy = BTSAMPLE_NOKEY;
	state->btss_want_itup = want_itup;
	state->btss_sample_itup = NULL;
	state->btss_sample_itupdesc = RelationGetDescr(index);
	state->btss_sample_offset_count_valid = false;
	ItemPointerSetInvalid(&state->btss_sample_tid);

	return state;
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
	state->btss_sample_offset_count_valid = false;
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
					state->btss_sample_offset_count_valid = true;
					state->btss_sample_offset = state->btss_sample_count = 0;
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
					state->btss_sample_offset_count_valid = true;
					state->btss_sample_offset = state->btss_sample_count = 0;
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
			state->btss_sample_offset_count_valid = true;
			state->btss_sample_offset = state->btss_sample_count = 0;
			return ;
		}
	}

	/* convert the search scankeys to insertion scankeys */
	if (state->btss_strategy & 0x5)
		_bt_sample_search_key_to_insertion_key(index, BTSS_LOWKEY);
	if (state->btss_strategy & 0x2)
		_bt_sample_search_key_to_insertion_key(index, BTSS_HIGHKEY);
} 

void _bt_destroy_sample_state(BTSampleState state) {
	if (state->btss_sample_itup != NULL)
		pfree(state->btss_sample_itup);
	pfree(state);	
}

#define _bt_sample_warn_concurrent_modification()	\
	do {	\
		ereport(WARNING,	\
				(errmsg("btsample has encountered concurrent modification")));	\
	} while(0)

#define _bt_sample_warn_ignore()	\
	do {	\
		ereport(WARNING,	\
				(errmsg("btsample has encountered deleted page and the tuple counts may be inconsistent")));	\
	} while(0)

#define _bt_sample_warn_empty_page()	\
	do {	\
		ereport(WARNING,	\
				(errmsg("btsample has encountered an empty page; the tuple counts may be inconsistent")));	\
	} while(0)

static
uint64 _bt_sample_count_all(Relation index) {
	Buffer		buf;
	uint64		result;

	buf = _bt_getroot(index, BT_READ);

	if (!BufferIsValid(buf)) {
		result = 0;
	}
	else {
		Page			page;
		BTPageOpaque	opaque;

		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		result = opaque->btpo_sum_tuple_count;

		_bt_relbuf(index, buf);
	}

	return result;	
}

static uint64 
_bt_sample_count_left(Relation index, ScanKey key) {
	Buffer	buf;
	uint64	total, result;

	buf = _bt_getroot(index, BT_READ);

	if (!BufferIsValid(buf)) {
		result = 0;
	} else {
		Page			page;
		BTPageOpaque	opaque;

		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		total = opaque->btpo_sum_tuple_count;

		key->sk_strategy = _bt_sample_opposite_strategy_number(key->sk_strategy);
		result = _bt_sample_count_right_impl(index, key, buf);
		key->sk_strategy = _bt_sample_opposite_strategy_number(key->sk_strategy);

		if (result > total) {
			_bt_sample_warn_concurrent_modification();
			result = 0;
		} else {
			result = total - result;
		}
	}

	return result;
}

static uint64 
_bt_sample_count_right(Relation index, ScanKey key) {
	Buffer	buf;
	uint64	result;

	buf = _bt_getroot(index, BT_READ);

	if (!BufferIsValid(buf)) {
		_bt_sample_warn_concurrent_modification();
		result = 0;
	}
	else {
		result = _bt_sample_count_right_impl(index, key, buf);
	}

	return result;
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
 * Find the first value that is >= key (if continue_val == 1)
 * or > key (if continue_val == 0).
 *
 */
static OffsetNumber
_bt_sample_binsrch(Relation index, Buffer buf,
				   ScanKey key, int32 continue_val) {
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
		/* this should not ever happen if there's no deletion. 
		 * The counts are not updated if tuples are deleted from
		 * a btree index. */
		_bt_sample_warn_empty_page();
		return low;
	}

	++high;

	while (low < high) {
		mid = low + ((high - low) >> 1);

		result = _bt_sample_compare(index, key, page, mid);
		if (result >= continue_val) {
			low = mid + 1;
		}
		else {
			high = mid;
		}
	}

	return low;
}

static uint64
_bt_sample_count_right_impl(Relation index, ScanKey key, Buffer buf) {
	uint64			result = 0;
	Page			page;
	BTPageOpaque	opaque;
	OffsetNumber	low,
					high,
					first;
	int32			continue_val;
	bool			concurrent_mod_warned = false;
	bool			ignore_warned = false;
	int32			cmp_result;
	ItemId			itemid;
	ItemPointerData rm_tid;		/* the rightmost downlink on the last page */
	IndexTuple		itup;
	BlockNumber		blkno;
	
	Assert(BufferIsValid(buf));	
	ItemPointerSetInvalid(&rm_tid);
	
	/* >= key => descend along the last link that has itemkey < key,
	 * > key => descend along the last link that has itemkey <= key,
	 *
	 * _bt_sample_compare compares key ? itemkey,
	 * continue the loop if cmp_result >= continue_val */
	continue_val = (key->sk_strategy == BTGreaterEqualStrategyNumber) ? 1 : 0;

	for (;;) {
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);

		low = P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);

		if (!P_RIGHTMOST(opaque)) {

			/* we don't want to handle the incomplete split */
			if (P_INCOMPLETE_SPLIT(opaque)) {
				if (!concurrent_mod_warned) {
					concurrent_mod_warned = true;
					_bt_sample_warn_concurrent_modification();
				}
			}

			if (P_IGNORE(opaque)) {
				if (!ignore_warned) {
					ignore_warned = true;
					_bt_sample_warn_ignore();
				}
				cmp_result = continue_val - 1;
			}
			else {
				cmp_result = _bt_sample_compare(index, key, page, P_HIKEY);
			}
			
			if (cmp_result >= continue_val) {

				/* go along the right link */
				buf = _bt_relandgetbuf(index, buf, opaque->btpo_next, BT_READ);
				continue;
			}
		}

		if (P_IGNORE(opaque)) {
			elog(ERROR, "fell off the end of index \"%s\"",
				RelationGetRelationName(index));
		}

			

		/* k_1 l_1 k_2 l_2 ... k_t l_t ... k_n l_n hikey
		 * where kt is the first itemkey that is >= (or >) key,
		 *
		 * the count from this subtree is subtree_count = 
		 * s_t + s_{t+1} + ...s_n = sum - s_1 - ... - s_t + subtree_count(l_t) */

		if (P_ISLEAF(opaque)) {
			/* leaf page tuples does not actually have tuple count field, 
			 * which defaults to 1. So it's okay to use binary search. */
				
			OffsetNumber off = _bt_sample_binsrch(index, buf, key, continue_val);
			result += high - off + 1;
			break;
		}

		else {
			/* on non-leaf page we have to use a linear search */

			Assert(low < high);

			result += opaque->btpo_sum_tuple_count;
			for (first = low; first <= high; ++first) {

				cmp_result = _bt_sample_compare(index, key, page, first);
				if (cmp_result >= continue_val) {
					/* go on */
					itemid = PageGetItemId(page, first);
					itup = (IndexTuple) PageGetItem(page, itemid);
					result -= index_tuple_get_count(itup);		
				}
				else {
					break;
				}
			}

			Assert(first > low);
			
			itemid = PageGetItemId(page, first - 1);
			itup = (IndexTuple) PageGetItem(page, itemid);
			blkno = ItemPointerGetBlockNumber(&(itup->t_tid));

			buf = _bt_relandgetbuf(index, buf, blkno, BT_READ);
		}
	}

	_bt_relbuf(index, buf);
	return result;
}

static
uint64 _bt_sample_count_range(BTSampleState state) {
	uint64 result;

	if (!state->btss_qual_ok) {
		return 0;
	}

	switch (state->btss_strategy) {
	case BTSAMPLE_NOKEY:
	{
		result = _bt_sample_count_all(state->btss_index);
	}
		break;

	case BTSAMPLE_LOWKEY:
	{
		result = _bt_sample_count_right(state->btss_index, BTSS_LOWKEY);
	}
		break;

	case BTSAMPLE_HIGHKEY:
	{
		result = _bt_sample_count_left(state->btss_index, BTSS_HIGHKEY);
	}
		break;

	case BTSAMPLE_RANGE:
	{
		uint64 result2;
		
		result = _bt_sample_count_right(state->btss_index, BTSS_LOWKEY);

		BTSS_HIGHKEY->sk_strategy = 
			_bt_sample_opposite_strategy_number(BTSS_HIGHKEY->sk_strategy);
		result2 = _bt_sample_count_right(state->btss_index, BTSS_HIGHKEY);
		BTSS_HIGHKEY->sk_strategy = 
			_bt_sample_opposite_strategy_number(BTSS_HIGHKEY->sk_strategy);
		if (result < result2) {
			result = 0;
		} else {
			result -= result2;
		}
	}
		break;

	case BTSAMPLE_EQUAL:
	{
		uint64 result2;
		
		BTSS_LOWKEY->sk_strategy = BTGreaterEqualStrategyNumber;
		result = _bt_sample_count_right(state->btss_index, BTSS_LOWKEY);
		
		BTSS_LOWKEY->sk_strategy = BTGreaterStrategyNumber;
		result2 = _bt_sample_count_right(state->btss_index, BTSS_LOWKEY);

		BTSS_LOWKEY->sk_strategy = BTEqualStrategyNumber;
		if (result < result2) {
			result = 0;
		} else {
			result -= result2;
		}
	}
		break;

	default:
		ereport(ERROR, (errmsg("btsample unknown sample strategy %d", 
							state->btss_strategy)));
	}

	return result;
}

static bool
_bt_sample_retrieve_nth_from_leftmost(BTSampleState state, uint64 n) {
	Buffer buf = _bt_getroot(state->btss_index, BT_READ);
	return _bt_sample_retrieve_nth_from_leftmost_impl(state, n, buf);
}

static bool
_bt_sample_retrieve_nth_from_leftmost_impl(BTSampleState state, uint64 n, Buffer buf) {
	Relation		index = state->btss_index;
	Page			page;
	BTPageOpaque	opaque;
	OffsetNumber	low,
					high,
					first;
	bool			concurrent_mod_warned = false;
	bool			ignore_warned = false;
	ItemId			itemid;
	IndexTuple		itup;
	BlockNumber		blkno = InvalidBlockNumber;	/* to suppress compiler warning */
	bool			result = false;
	uint64			tuple_count;
	
	if (!BufferIsValid(buf)) {
		return false;
	}
	
	for (; n >= 0 ;) {
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);

		low = P_FIRSTDATAKEY(opaque);
		high = PageGetMaxOffsetNumber(page);

		if (!P_RIGHTMOST(opaque)) {
			if (P_INCOMPLETE_SPLIT(opaque)) {
				if (!concurrent_mod_warned) {
					concurrent_mod_warned = true;
					_bt_sample_warn_concurrent_modification();
				}
			}

			if (P_IGNORE(opaque)) {
				if (!ignore_warned) {
					ignore_warned = true;
					_bt_sample_warn_ignore();
				}
			}
			
			if (n >= opaque->btpo_sum_tuple_count) {
				n -= opaque->btpo_sum_tuple_count;
				buf = _bt_relandgetbuf(index, buf, opaque->btpo_next, BT_READ);
			}
		}

		if (P_IGNORE(opaque)) {
			elog(ERROR, "fell off the end of index \"%s\"",
				RelationGetRelationName(index));
		}

		if (P_ISLEAF(opaque)) {
			if (low + n > high) {
				result = false;
			}
			else {
				itemid = PageGetItemId(page, low + n);
				itup = (IndexTuple) PageGetItem(page, itemid);

				state->btss_sample_tid = itup->t_tid;
				if (state->btss_want_itup) {
					if (state->btss_sample_itup)
						pfree(state->btss_sample_itup);
					state->btss_sample_itup = CopyIndexTuple(itup);
				}

				result = true;	
			}

			break;
		}
		else {
			for (first = low; first <= high; ++first) {
				itemid = PageGetItemId(page, first);
				itup = (IndexTuple) PageGetItem(page, itemid);

				tuple_count = index_tuple_get_count(itup);	
				if (n < tuple_count) {
					blkno = ItemPointerGetBlockNumber(&(itup->t_tid));
					break;
				}
				else {
					n -= tuple_count;
				}
			}

			if (first == high + 1) {
				result = false;
				break;
			}
			
			buf = _bt_relandgetbuf(index, buf, blkno, BT_READ);
		}
	}

	_bt_relbuf(index, buf);
	return result;
}

BTSampleState _bt_prepare_sample_state(Relation index, int nkeys, ScanKey keys,
									   bool want_itup) {
	BTSampleState state;

	state = _bt_init_sample_state(index, want_itup);
	_bt_sample_set_key(state, nkeys, keys);

	return state;
}

bool _bt_sample(BTSampleState state) {
	bool			result = true;
	uint64			offset,
					count;
	
	if (!state->btss_qual_ok) return false;

	if (!state->btss_sample_offset_count_valid) {
		Page			page;
		BTPageOpaque	opaque;
		Buffer			buf;
		uint64			total_count,
						low_right_count,
						high_right_count;

		switch (state->btss_strategy) {
		case BTSAMPLE_NOKEY:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}

			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			offset = 0;
			count = opaque->btpo_sum_tuple_count;

			if (count < 0) {
				result = false;
				break;
			}

            _bt_relbuf(state->btss_index, buf);
		}
			break;

		case BTSAMPLE_LOWKEY:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}

			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			total_count = opaque->btpo_sum_tuple_count;
			count = _bt_sample_count_right_impl(state->btss_index, 
													   BTSS_LOWKEY,
													   buf);
			offset = total_count - count;

			if (total_count < count) {
				result = false;
				break;
			}
		}
			break;

		case BTSAMPLE_HIGHKEY:
		{
			offset = 0;
			count = _bt_sample_count_left(state->btss_index,
												 BTSS_HIGHKEY);
			if (count < 0) {
				result = false;
				break;
			}
		}
			break;

		case BTSAMPLE_RANGE:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}
			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			total_count = opaque->btpo_sum_tuple_count;
			low_right_count = _bt_sample_count_right_impl(state->btss_index,
																 BTSS_LOWKEY,
																 buf);

			BTSS_HIGHKEY->sk_strategy = 
				_bt_sample_opposite_strategy_number(BTSS_HIGHKEY->sk_strategy);
			high_right_count =  _bt_sample_count_right(state->btss_index, 
													   BTSS_HIGHKEY);
			BTSS_HIGHKEY->sk_strategy = 
				_bt_sample_opposite_strategy_number(BTSS_HIGHKEY->sk_strategy);
			
			if (low_right_count < high_right_count) {
				result = false;
				break;
			}
			if (total_count < low_right_count) {
				result = false;
				break;
			}
			count = low_right_count - high_right_count;
			offset = total_count - low_right_count;

		}
			break;

		case BTSAMPLE_EQUAL:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}

			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			total_count = opaque->btpo_sum_tuple_count;
			
			BTSS_LOWKEY->sk_strategy = BTGreaterEqualStrategyNumber;
			low_right_count = _bt_sample_count_right_impl(state->btss_index,
														  BTSS_LOWKEY,
														  buf);

			BTSS_LOWKEY->sk_strategy = BTGreaterStrategyNumber;
			high_right_count =  _bt_sample_count_right(state->btss_index, 
													   BTSS_LOWKEY);
			BTSS_LOWKEY->sk_strategy = BTEqualStrategyNumber;
			
			if (low_right_count < high_right_count) {
				result = false;
				break;
			}
			if (total_count < low_right_count) {
				result = false;
				break;
			}
			count = low_right_count - high_right_count;
			offset = total_count - low_right_count;

		}
			break;

		default:
			ereport(ERROR, (errmsg("unknown btsample strategy %d", state->btss_strategy)));
			result = false;
		}
		
		if (result) {
			state->btss_sample_offset_count_valid = true;
			state->btss_sample_offset = offset;
			state->btss_sample_count = count;
		}
	}
	else {
		offset = state->btss_sample_offset;
		count = state->btss_sample_count;
	}
	

	if (result) {
		uint64 rnd;
		
		if (count == 0) {
			ItemPointerSetInvalid(&state->btss_sample_tid);
			result = true;
		}
		else {
			rnd = _bt_sample_randint(count - 1) + offset;
			result = _bt_sample_retrieve_nth_from_leftmost(state, rnd);
		}
	}

	return result;
}

extern bool _bt_count(BTSampleState state) {
	bool			result = true;
	uint64			offset,
					count;
	
	if (!state->btss_qual_ok) return false;

	if (!state->btss_sample_offset_count_valid) {
		Page			page;
		BTPageOpaque	opaque;
		Buffer			buf;
		uint64			total_count,
						low_right_count,
						high_right_count;

		switch (state->btss_strategy) {
		case BTSAMPLE_NOKEY:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}

			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			offset = 0;
			count = opaque->btpo_sum_tuple_count;

			if (count < 0) {
				result = false;
				break;
			}

            _bt_relbuf(state->btss_index, buf);
		}
			break;

		case BTSAMPLE_LOWKEY:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}

			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			total_count = opaque->btpo_sum_tuple_count;
			count = _bt_sample_count_right_impl(state->btss_index, 
													   BTSS_LOWKEY,
													   buf);
			offset = total_count - count;

			if (total_count < count) {
				result = false;
				break;
			}
		}
			break;

		case BTSAMPLE_HIGHKEY:
		{
			offset = 0;
			count = _bt_sample_count_left(state->btss_index,
												 BTSS_HIGHKEY);
			if (count < 0) {
				result = false;
				break;
			}
		}
			break;

		case BTSAMPLE_RANGE:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}
			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			total_count = opaque->btpo_sum_tuple_count;
			low_right_count = _bt_sample_count_right_impl(state->btss_index,
																 BTSS_LOWKEY,
																 buf);

			BTSS_HIGHKEY->sk_strategy = 
				_bt_sample_opposite_strategy_number(BTSS_HIGHKEY->sk_strategy);
			high_right_count =  _bt_sample_count_right(state->btss_index, 
													   BTSS_HIGHKEY);
			BTSS_HIGHKEY->sk_strategy = 
				_bt_sample_opposite_strategy_number(BTSS_HIGHKEY->sk_strategy);
			
			if (low_right_count < high_right_count) {
				result = false;
				break;
			}
			if (total_count < low_right_count) {
				result = false;
				break;
			}
			count = low_right_count - high_right_count;
			offset = total_count - low_right_count;

		}
			break;

		case BTSAMPLE_EQUAL:
		{
			buf = _bt_getroot(state->btss_index, BT_READ);
			if (!BufferIsValid(buf)) {
				result = false;
				break;
			}

			page = BufferGetPage(buf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
			total_count = opaque->btpo_sum_tuple_count;
			
			BTSS_LOWKEY->sk_strategy = BTGreaterEqualStrategyNumber;
			low_right_count = _bt_sample_count_right_impl(state->btss_index,
														  BTSS_LOWKEY,
														  buf);

			BTSS_LOWKEY->sk_strategy = BTGreaterStrategyNumber;
			high_right_count =  _bt_sample_count_right(state->btss_index, 
													   BTSS_LOWKEY);
			BTSS_LOWKEY->sk_strategy = BTEqualStrategyNumber;
			
			if (low_right_count < high_right_count) {
				result = false;
				break;
			}
			if (total_count < low_right_count) {
				result = false;
				break;
			}
			count = low_right_count - high_right_count;
			offset = total_count - low_right_count;

		}
			break;

		default:
			ereport(ERROR, (errmsg("unknown btsample strategy %d", state->btss_strategy)));
			result = false;
		}
		
		if (result) {
			state->btss_sample_offset_count_valid = true;
			state->btss_sample_offset = offset;
			state->btss_sample_count = count;
		}
	}
	else {
		offset = state->btss_sample_offset;
		count = state->btss_sample_count;
	}

	return result;
}


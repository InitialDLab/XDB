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

OnlineAggState *ExecInitOnlineAgg(OnlineAgg *node, EState *estate, int eflags) {
	ereport(ERROR, (errmsg("fix executor of onlineagg")));
}
OnlineSampleJoinState *ExecInitOnlineSampleJoin(OnlineSampleJoin *node, 
													   EState *estate, int eflags){}
TupleTableSlot *ExecOnlineAgg(OnlineAggState *state) {}
TupleTableSlot *ExecOnlineSampleJoin(OnlineSampleJoinState *state) {}
void ExecEndOnlineAgg(OnlineAggState *state) {}
void ExecEndOnlineSampleJoin(OnlineSampleJoinState *state) {}

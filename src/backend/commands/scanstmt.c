#include "commands/scanstmt.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h"
#include "access/heapam.h"

static Oid _ss_get_rel_oid(const RangeVar *rv);
static void _ss_scan_rel(Oid relid);

void run_scan_stmt(ScanStmt *scanStmt) {
  MemoryContext oldcontext;
  MemoryContext newcontext = AllocSetContextCreate(CurrentMemoryContext,
      "RunScanStmtContext",
      ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE,
      ALLOCSET_DEFAULT_MAXSIZE);
  Oid relid;
  
  oldcontext = MemoryContextSwitchTo(newcontext);

  relid = _ss_get_rel_oid(scanStmt->relation);

  _ss_scan_rel(relid);

  MemoryContextSwitchTo(oldcontext);
  MemoryContextDelete(newcontext);
}

static Oid
_ss_get_rel_oid(const RangeVar *rv) {
  
  Oid oid = RangeVarGetRelid(rv, NoLock, false);
  return oid;
}

static void
_ss_scan_rel(Oid relid) {
  
  Relation rel = relation_open(relid, AccessShareLock);
  BlockNumber curBlkNum;
  BlockNumber numBlocks = RelationGetNumberOfBlocks(rel);

  for (curBlkNum = 0; curBlkNum < numBlocks; ++curBlkNum) {
    Buffer buf = ReadBuffer(rel, curBlkNum);
    ReleaseBuffer(buf);
  }

  relation_close(rel, AccessShareLock);
}


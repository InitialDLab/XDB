#ifndef SCANSTMT_H
#define SCANSTMT_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "catalog/namespace.h"

extern void run_scan_stmt(ScanStmt *scanStmt);

#endif /* SCANSTMT_H */

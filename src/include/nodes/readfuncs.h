/*-------------------------------------------------------------------------
 *
 * readfuncs.h
 *	  header file for read.c and readfuncs.c. These functions are internal
 *	  to the stringToNode interface and should not be used by anyone else.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/readfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef READFUNCS_H
#define READFUNCS_H

#include "nodes/nodes.h"

#define RIGHT_PAREN (1000000 + 1)
#define LEFT_PAREN	(1000000 + 2)
#define LEFT_BRACE	(1000000 + 3)
#define OTHER_TOKEN (1000000 + 4)

/*
 * prototypes for functions in read.c (the lisp token parser)
 */
extern char *pg_strtok(int *length);
extern char *debackslash(char *token, int length);
extern void *nodeRead(char *token, int tok_len);
extern NodeTag nodeTokenType(char *token, int length);

/*
 * prototypes for functions in readfuncs.c
 */
extern Node *parseNodeString(void);

#endif   /* READFUNCS_H */

#ifndef _H_AVLTREE
#define _H_AVLTREE

#include "postgres.h"
#include "executor/tuptable.h"

typedef int (*avl_callback_fn)(TupleTableSlot *x, TupleTableSlot *y, void *z);

typedef struct avlnode
{
	TupleTableSlot *slot;
	double weight;
	double sum; /* sum of weights in the subtree */
	struct avlnode *data;
	int height;

	uint64 seen;
	uint64 total;
	
	struct avlnode *parent;
	struct avlnode *left;
	struct avlnode *right;
} avlnode;

/* create new node */
avlnode *avl_create_node(TupleTableSlot *key, double weight, avlnode *data);

/* free the avl tree */
void avl_clean(avlnode *t);

/* insert new node */
avlnode* avl_insert(avlnode *t, avlnode *tmp, avl_callback_fn cmp, void *cxt);

/* find by key */
avlnode *avl_find(avlnode *t, TupleTableSlot *tmp, avl_callback_fn cmp, void *cxt);

/* find by prefix sum of weights */
avlnode *avl_find_weight(avlnode *t, double tmp);

void avl_maintain_sum(avlnode * t, double newweight);

/* display the avl tree */
void avl_display(avlnode *t);

#define avl_get_sum(n) ((n) ? (n)->sum : 0)

uint64 validate_tree(avlnode *root);

#endif

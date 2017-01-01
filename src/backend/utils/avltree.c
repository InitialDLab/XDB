#include "postgres.h"

#include "utils/avltree.h"
#include "utils/palloc.h"
#include <stdio.h>

#ifdef PG_USE_INLINE
#define AVL_INLINE inline
#else
#define AVL_INLINE 
#endif

#define get_sum(n) avl_get_sum(n)
#define get_height(n) ((n) ? (n)->height : 0)
#define get_balance(n) (get_height(n->left) - get_height(n->right))

#define set_parent(c, p) ((c) ? ((void) (c->parent = (p))) : \
		((void) 0))

static AVL_INLINE int 
max(int x, int y)
{
	return (x>y) ? x : y;
}

static AVL_INLINE avlnode *
avl_r_rotate(avlnode *t)
{
	avlnode *tmp = t->left;
	t->left = tmp->right;
	tmp->right = t;
	
	set_parent(tmp, t->parent);
	set_parent(t, tmp);
	set_parent(t->left, t);

	t->height = max(get_height(t->left), get_height(t->right)) + 1;
	t->sum = get_sum(t->left) + get_sum(t->right) + t->weight;
	
	tmp->height = max(get_height(tmp->left), get_height(tmp->right)) + 1;
	tmp->sum = get_sum(tmp->left) + get_sum(tmp->right) + tmp->weight;

	return tmp;
}

static AVL_INLINE avlnode *
avl_l_rotate(avlnode *t)
{
	avlnode *tmp = t->right;
	t->right = tmp->left;
	tmp->left = t;
	
	set_parent(tmp, t->parent);
	set_parent(t, tmp);
	set_parent(t->right, t);

	t->height = max(get_height(t->left), get_height(t->right)) + 1;
	t->sum = get_sum(t->left) + get_sum(t->right) + t->weight;
	
	tmp->height = max(get_height(tmp->left), get_height(tmp->right)) + 1;
	tmp->sum = get_sum(tmp->left) + get_sum(tmp->right) + tmp->weight;

	return tmp;
}


/* XXX key and data fields are not freed */
void avl_clean(avlnode *t)
{
	if(t != NULL)
	{
		if(t->left != NULL)
			avl_clean(t->left);
		if(t->right != NULL)
			avl_clean(t->right);
		pfree(t);
	}
}


avlnode *avl_create_node(TupleTableSlot * x, double y, avlnode *data)
{
	avlnode *tmp = (avlnode*) palloc(sizeof(avlnode));
	if(tmp == NULL)
		return NULL;
	else
	{
		tmp->slot = x;
		tmp->weight = y;
		tmp->sum = y;
		tmp->data = data;
		tmp->parent = tmp->left = tmp->right = NULL;
		tmp->height = 1;
		tmp->total = tmp->seen = 0;
		return tmp;
	}
}

avlnode * 
avl_insert(avlnode *t, avlnode *tmp, avl_callback_fn cmp, void *cxt)
{
	int balance;
	int cmp_result;

	if(t == NULL)
	{
		return tmp;
	}

	cmp_result = cmp(tmp->slot, t->slot, cxt);
	if (cmp_result == 0) {
		ereport(ERROR, (errmsg("cannot handle duplicate keys in the avltree impl.")));
	}
	else if(cmp_result < 0)
	{
		t->left = avl_insert(t->left, tmp, cmp, cxt);
		t->left->parent = t;
		t->sum += tmp->weight;
	}
	else if(cmp_result > 0)
	{
		t->right = avl_insert(t->right, tmp, cmp, cxt);
		t->right->parent = t;
		t->sum += tmp->weight;
	}
	t->height = max(get_height(t->left), get_height(t->right)) + 1;
	
	balance = get_balance(t);
	if (balance == 2) {
		Assert(t->left != NULL);
		if (get_balance(t->left) == -1) {
			t->left = avl_l_rotate(t->left);
		}
		else {
			Assert(get_balance(t->left) == 1);
		}
		t = avl_r_rotate(t);
	}
	else if (balance == -2) {
		Assert(t->right != NULL);
		if (get_balance(t->right) == 1) {
			t->right = avl_r_rotate(t->right);
		}
		else {
			Assert(get_balance(t->right) == -1);
		}
		t = avl_l_rotate(t);
	}

	Assert(!t->left || t->left->parent == t);
	Assert(!t->right || t->right->parent == t);

	return t;
}

avlnode *
avl_find(avlnode *t, TupleTableSlot * tmp, avl_callback_fn cmp, void *cxt)
{
	int cmp_res; 
	
	if (t == NULL) return NULL;
	cmp_res = cmp(tmp, t->slot, cxt);
	if(cmp_res == 0)
		return t;
	else if(cmp_res < 0)
		return avl_find(t->left, tmp, cmp, cxt);
	else
		return avl_find(t->right, tmp, cmp, cxt);
}

avlnode *
avl_find_weight(avlnode *t, double tmp)
{
	double lsum = ((t->left) ? t->left->sum : 0);
	if (tmp <= lsum)
		return avl_find_weight(t->left, tmp);
	if (tmp <= t->weight + lsum)
		return t;
	
	Assert(t->right != NULL);
	tmp -= t->weight + lsum;
	if (tmp < 0) tmp = 0;
	return avl_find_weight(t->right, tmp);
}

void
avl_maintain_sum(avlnode *t, double newweight) {
	if (t->weight == newweight) return ;
	
	t->weight = newweight;
	while (t != NULL) {
		t->sum = get_sum(t->left) + get_sum(t->right) + t->weight;
		t = t->parent;
	}
}

/*
int main()
{
	avlnode *root = NULL;
	avlnode *node1 = avl_create_node(1, 1);
	avlnode *node2 = avl_create_node(2, 1);
	avlnode *node3 = avl_create_node(3, 1);
	avlnode *node4 = avl_create_node(4, 1);
	root = avl_insert(root, node1, compare_datum);
	avl_display(root);
	root = avl_insert(root, node2, compare_datum);
	avl_display(root);
	root = avl_insert(root, node3, compare_datum);
	avl_display(root);
	root = avl_insert(root, node4, compare_datum);
	avl_display(root);
	return 0;
}
*/

uint64 validate_tree(avlnode *t) {
	uint64 dc, lc, rc;
	double expected_sum, expected_weight;
	if (t == NULL) return 0;
	
	/* validate data */
	if (t->data == t) {
		/* there's no tuples that join */
		if (t->total != 0 || t->seen != 0 || abs(t->weight) > 1e-10)
			elog(ERROR, "corrupted avl tree weight, count and total");
	}
	else if (t->data) {
		dc = validate_tree(t->data);
		if (dc != t->seen) {
			elog(ERROR, "corrupted avl tree data count");
		}

		if (t->seen != 0) {
			expected_weight = get_sum(t->data) * t->total / t->seen;
			if (abs(t->weight - expected_weight) > 1e-10) 
				elog(ERROR, "corrupted avl tree weight, seen != 0");
		}
		else {
			if (abs(t->weight) > 1e-10)
				elog(ERROR, "corrupted avl tree weight, seen = 0");
		}
	}
	
	/* validate child nodes */
	lc = validate_tree(t->left);
	rc = validate_tree(t->right);
	
	expected_sum = t->weight + get_sum(t->left) + get_sum(t->right);
	if (abs(t->sum - expected_sum) > 1e-10) {
		elog(ERROR, "corrupted avl tree sum");
	}

	return lc + rc + 1;
}


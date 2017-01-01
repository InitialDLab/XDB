\timing on
SELECT ONLINE c_mktsegment, SUM(l_extendedprice * (1 - l_discount))
FROM customer, lineitem, orders, nation
WHERE	c_custkey = o_custkey
	AND	l_orderkey = o_orderkey
	AND l_returnflag = 'R'
	AND c_nationkey = n_nationkey
GROUP BY c_mktsegment
WITHTIME 60000 CONFIDENCE 95 REPORTINTERVAL 1000;

\timing on
SELECT c_mktsegment, SUM(l_extendedprice * (1 - l_discount))
FROM customer, lineitem, orders , nation
WHERE	c_custkey = o_custkey
	AND	l_orderkey = o_orderkey
	AND l_returnflag = 'R'
	AND c_nationkey = n_nationkey
	AND l_shipdate >= date '1994-05-04'
	AND l_shipdate < date '1994-05-04' + interval '12' month
GROUP BY c_mktsegment;

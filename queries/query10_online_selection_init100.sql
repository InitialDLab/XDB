\timing on
SELECT ONLINE SUM(l_extendedprice * (1 - l_discount))
FROM customer, lineitem, orders , nation
WHERE	c_custkey = o_custkey
	AND	l_orderkey = o_orderkey
	AND c_nationkey = n_nationkey
	AND l_shipdate >= date '1994-05-04'
	AND l_shipdate < date '1994-05-04' + interval '12' month
	AND l_returnflag = 'R'
WITHTIME 120000 CONFIDENCE 95 REPORTINTERVAL 1000
INITSAMPLE 100;

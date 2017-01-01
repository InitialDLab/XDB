\timing on
SELECT ONLINE SUM(l_extendedprice * (1 - l_discount))
FROM supplier, lineitem, orders, customer, nation n2, nation n1
WHERE   s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
	AND n1.n_name = 'CHINA'
    AND l_shipdate > date '1994-05-01'
WITHTIME 120000 CONFIDENCE 95 REPORTINTERVAL 1000
INITSAMPLE 100;

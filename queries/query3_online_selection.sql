\timing on
SELECT ONLINE SUM(l_extendedprice * (1 - l_discount))
FROM lineitem, orders, customer
WHERE   c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_shipdate > date '1995-06-01'
WITHTIME 120000 CONFIDENCE 95 REPORTINTERVAL 1000;

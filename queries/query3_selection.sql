\timing on

--SELECT COUNT(*)
--FROM customer, orders, lineitem
--WHERE   c_mktsegment = 'BUILDING'
--    AND c_custkey = o_custkey
--    AND l_orderkey = o_orderkey;

--SELECT COUNT(*)
--FROM customer, orders, lineitem
--WHERE   c_mktsegment = 'BUILDING'
--    AND c_custkey = o_custkey
--    AND l_orderkey = o_orderkey
--    AND l_shipdate > date '1997-06-01';

SELECT SUM(l_extendedprice * (1 - l_discount))
FROM customer, orders, lineitem
WHERE   c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_shipdate > date '1995-06-01';

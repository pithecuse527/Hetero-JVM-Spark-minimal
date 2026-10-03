-- TPC-H q3, UDF variant. Identical to tpch/q3.sql except the revenue expression
-- `l_extendedprice * (1 - l_discount)` is replaced by the compute-heavy
-- price_score(l_extendedprice, l_discount) UDF (see PriceUdf in ScreeningJob.scala).
-- Run with: query arg "q3-udf". Intensity: --conf spark.gcaware.udf.iters=<n>.
select
	l_orderkey,
	sum(price_score(l_extendedprice, l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10

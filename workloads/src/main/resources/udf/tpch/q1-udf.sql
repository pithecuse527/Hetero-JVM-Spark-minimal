-- TPC-H q1, UDF variant. Identical to tpch/q1.sql except the sum_disc_price
-- expression `l_extendedprice * (1 - l_discount)` is replaced by the compute-heavy
-- price_score(l_extendedprice, l_discount) UDF. q1 is a pure scan+aggregate over
-- lineitem (NO joins), so the UDF runs on ~all of lineitem (max call count) and
-- there is almost no join/sort floor to dilute the JIT effect. Run: query "q1-udf".
-- Intensity: --conf spark.gcaware.udf.iters=<n> (keep LOW here — ~1.2B calls at SF200).
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(price_score(l_extendedprice, l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus

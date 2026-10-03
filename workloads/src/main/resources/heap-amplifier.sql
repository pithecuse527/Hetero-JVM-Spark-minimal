-- The UDF is applied only to rows produced by the join and GROUP BY.
-- ${GROUP_COUNT}, ${DATE_START}, and ${DATE_END} are validated and substituted
-- by HeapArrayAmplifierRunner before spark.sql executes this statement.
WITH grouped AS (
  SELECT
    pmod(o.o_orderkey, ${GROUP_COUNT}) AS group_key,
    sum(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue
  FROM lineitem l
  JOIN orders o ON l.l_orderkey = o.o_orderkey
  WHERE l.l_shipdate >= date '${DATE_START}'
    AND l.l_shipdate < date '${DATE_END}'
  GROUP BY pmod(o.o_orderkey, ${GROUP_COUNT})
)
SELECT
  sum(heap_array_amplify(revenue, cast(group_key AS bigint))) AS amplified_revenue,
  count(*) AS groups
FROM grouped

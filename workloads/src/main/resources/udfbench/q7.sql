-- file_q7 -> file_json3: reads pubmed_q7.txt (json array) as rows of (c1,c2,c3).
SELECT aggregate_avg(jsoncount(r.c2)), aggregate_avg(jsoncount(r.c3))
FROM (SELECT explode(file_json3('pubmed_q7.txt')) AS r)

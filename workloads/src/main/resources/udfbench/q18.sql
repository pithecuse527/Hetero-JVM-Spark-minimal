-- aggregate_top (ex-UDTF, per-group top-N) -> row_number() window, rn<=5.
-- file_q18 -> file_csv2 (arxiv.csv) / file_json2 (pubmed.txt). stem/filterstopwords real.
SELECT arxivid, pubmedid, similarity AS top_s
FROM (
  SELECT arxivid, pubmedid, similarity,
         row_number() OVER (PARTITION BY arxivid ORDER BY similarity DESC) AS rn
  FROM (
    SELECT arxivid, pubmedid, JACCARD(arxivterms, pmcterms) AS similarity
    FROM (
      SELECT arxivid,
             JPACK(FREQUENTTERMS(STEM(FILTERSTOPWORDS(KEYWORDS(abstract))), 10)) AS arxivterms
      FROM (SELECT r.c1 AS arxivid, r.c2 AS abstract
            FROM (SELECT explode(file_csv2('arxiv.csv')) AS r)) xx
    ) xxx,
    (
      SELECT pubmedid,
             JPACK(FREQUENTTERMS(STEM(FILTERSTOPWORDS(KEYWORDS(abstract))), 10)) AS pmcterms
      FROM (SELECT r.c1 AS pubmedid, r.c2 AS abstract
            FROM (SELECT explode(file_json2('pubmed.txt')) AS r)) zz
    ) zzz
  ) inner_join
  WHERE arxivid IS NOT NULL AND pubmedid IS NOT NULL AND similarity IS NOT NULL
) ranked
WHERE rn <= 5

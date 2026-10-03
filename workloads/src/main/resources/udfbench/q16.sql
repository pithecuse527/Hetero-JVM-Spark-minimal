-- combinations_q16 (ex-UDTF) -> reuse combinations (array of json pairs) with the 7
-- passthrough columns carried by LATERAL VIEW explode. `class` -> `fclass` (reserved word).
WITH base AS (
  SELECT pubid, pubdate, projectstart, projectend, funder, fclass, projectid, authorlist
  FROM (
    select artifact_authorlists.artifactid as pubid, artifacts.date as pubdate,
           projects.startdate as projectstart, projects.enddate as projectend,
           extractfunder(projects.fundingstring) AS funder,
           extractclass(projects.fundingstring) AS fclass,
           extractid(projects.fundingstring) AS projectid,
           jsort(jsortvalues(removeshortterms(lowerize(artifact_authorlists.authorlist)))) as authorlist
    from projects, projects_artifacts, artifacts, artifact_authorlists
    where projects.id = projects_artifacts.projectid
      and projects_artifacts.artifactid = artifact_authorlists.artifactid
      and projects_artifacts.artifactid = artifacts.id
      and jsoncount(authorlist) < 7
  ) x
),
pairs AS (
  SELECT pubid, pubdate, projectstart, projectend, funder, fclass, projectid, authorpair
  FROM base LATERAL VIEW explode(combinations(authorlist, 2)) t AS authorpair
)
SELECT funder, fclass, projectid,
  SUM(CASE WHEN cleandate(pubdate) between pstartcleaned and pendcleaned
      THEN 1 ELSE NULL END) AS authors_during,
  SUM(CASE WHEN cleandate(pubdate) < pstartcleaned
      THEN 1 ELSE NULL END) AS authors_before,
  SUM(CASE WHEN cleandate(pubdate) > pendcleaned
      THEN 1 ELSE NULL END) AS authors_after
FROM (
  SELECT projectpairs.funder, projectpairs.fclass, projectpairs.projectid,
         cleandate(projectpairs.projectstart) AS pstartcleaned,
         cleandate(projectpairs.projectend) AS pendcleaned,
         pairs.authorpair, pairs.pubdate
  FROM (SELECT * FROM pairs WHERE projectid IS NOT NULL) AS projectpairs, pairs
  WHERE projectpairs.authorpair = pairs.authorpair
) AS xx
GROUP BY funder, fclass, projectid

-- file_q13('crossref.xml','text')+xmlparser -> xml_records (array of json dicts);
-- extractkeys (ex-UDTF) -> array of struct(publicationdoi,fundinginfo); LATERAL VIEW explode.
SELECT artifacts.id, crossref.projectid
FROM (
  SELECT k.publicationdoi AS publicationdoi, extractprojectid(k.fundinginfo) AS projectid
  FROM (SELECT explode(xml_records('crossref.xml', 'publication')) AS rec) r
       LATERAL VIEW explode(extractkeys(rec, 'publicationdoi', 'fundinginfo')) e AS k
) AS crossref, artifacts
WHERE publicationdoi = artifacts.id
AND crossref.projectid NOT IN (
  SELECT extractcode(fundingstring) AS projectid
  FROM projects_artifacts, projects
  WHERE projects_artifacts.artifactid = artifacts.id
    AND projects.id = projects_artifacts.projectid
)

-- file_q13('crossref.txt','text') -> file_text (array of lines);
-- jsonparse (ex-UDTF, 1:1) -> jsonparse_pair struct(publicationdoi,fundinginfo).
SELECT p.publicationdoi AS publicationdoi, extractprojectid(p.fundinginfo) AS projectid
FROM (
  SELECT jsonparse_pair(column1, 'publicationdoi', 'fundinginfo') AS p
  FROM (SELECT explode(file_text('crossref.txt')) AS column1)
) AS crossref

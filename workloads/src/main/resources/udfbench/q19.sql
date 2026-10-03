select pid, datasets, other, publications, software
from (
  select pr.projectid as pid, r.type as result_type
  from (select id, type from artifacts) r
  join projects_artifacts pr on r.id = pr.artifactid
)
pivot (
  count(1) for result_type in
    ('dataset' as datasets, 'other' as other, 'publication' as publications, 'software' as software)
)

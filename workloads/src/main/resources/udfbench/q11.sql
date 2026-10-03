select c.clusterid, c.id, t.type, c.points
from (
  select type, kmeans(collect_list(concat(id, '\t', cast(fundedamount as string))), 5, 30) as clusters
  from (
    select id, type, sum(fundedamount) as fundedamount
    from (
      select artifacts.id as id, artifacts.type as type,
             converttoeuro(projects.fundedamount, projects.currency) as fundedamount
      from artifacts, projects, projects_artifacts
      where artifacts.id = projects_artifacts.artifactid
        and projects_artifacts.projectid = projects.id
        and projects.fundedamount > 0.0
    ) group by id, type
  ) group by type
) t
lateral view explode(t.clusters) e as c

-- combinations (ex-UDTF) returns an array of json author-pairs; LATERAL VIEW explode.
select aggregate_count(authorpairs)
from (select clean(authorlist) as cl
      from artifact_authorlists
      where jsoncount(authorlist) <= 50) zz
lateral view explode(combinations(cl, 2)) t as authorpairs

with total_docid as (select count(distinct artifactid) as doc_count from artifact_abstracts),
processed as (
  select artifactid as docid, stem(filterstopwords(keywords(lowerize(abstract)))) as abstract
  from artifact_abstracts
),
terms as (
  select docid, term from processed lateral view explode(strsplitv(abstract)) t as term
),
tf as (
  select term, docid,
         (1.0*count(*)) / (1.0 * sum(count(*)) over (partition by docid)) as tf
  from terms group by term, docid
),
jg as (
  select term, docid, tf, count(*) over (partition by term) as jcount from tf
)
select docid, term, tf*(log_10(((select max(doc_count) from total_docid)*1.0)/(1.0+jcount))+1.0) as tfidf
from jg

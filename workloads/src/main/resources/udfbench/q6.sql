select doi, amount, totalpubs, sdate from (
  select get_json_object(rec,'$.doi') as doi,
         cast(get_json_object(rec,'$.amount') as double) as amount,
         cast(get_json_object(rec,'$.totalpubs') as int) as totalpubs,
         get_json_object(rec,'$.sdate') as sdate
  from (select explode(xml_records('arxiv.xml','publication')) as rec)
  union all
  select get_json_object(line,'$.doi') as doi,
         cast(get_json_object(line,'$.amount') as double) as amount,
         cast(get_json_object(line,'$.totalpubs') as int) as totalpubs,
         get_json_object(line,'$.sdate') as sdate
  from (select explode(file_text('query2json.txt')) as line)
)

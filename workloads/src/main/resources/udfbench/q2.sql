-- extractfromdate (ex-UDTF, 1:1) returns struct(year,month,day); id passthrough.
SELECT id, s.year AS year, s.month AS month, s.day AS day
FROM (SELECT id, extractfromdate(date) AS s FROM artifacts)

a1 = LOAD '/user/hadoop/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' AS (ngram:chararray,year:int,occurrences:float,books:float);
a2 = GROUP a1 BY ngram;
a3 = FOREACH a2 GENERATE group,SUM(a1.occurrences) AS total_occur,SUM(a1.books) AS total_books,SUM(a1.occurrences)/SUM(a1.books) AS avg_occur,MIN(a1.year) AS min_year,MAX(a1.year) AS max_year,COUNT(a1.year) as records;
a4 = FILTER a3 BY ((min_year == 1950) AND (records == 60));
a5 = ORDER a4 BY avg_occur DESC;
a6 = LIMIT a5 10;
STORE a6 INTO 'files';

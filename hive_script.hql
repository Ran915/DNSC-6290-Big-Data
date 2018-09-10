create external table a1 (
ngram string, 
year int,
occurrences float,
books float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hadoop/store';

LOAD DATA INPATH '/user/hadoop/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' OVERWRITE INTO TABLE a1;

INSERT OVERWRITE DIRECTORY 'hives'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT ngram,SUM(occurrences) AS total_occur,SUM(books) AS total_books,SUM(occurrences)/SUM(books) AS avg_occur,MIN(year) AS min_year,MAX(year) AS max_year,COUNT(year) as records
FROM a1
GROUP BY ngram
HAVING min_year==1950 AND records==60
ORDER BY avg_occur DESC
LIMIT 10;

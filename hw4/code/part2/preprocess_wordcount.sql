CREATE VIEW  IF NOT EXISTS twitter_analysis.data as
SELECT time, count as data
FROM `hardy-symbol-252200.twitter_analysis.wordcount`
WHERE word="data";

CREATE VIEW IF NOT EXISTS twitter_analysis.ai as
SELECT time, count as ai
FROM `hardy-symbol-252200.twitter_analysis.wordcount`
WHERE word="ai";

CREATE VIEW  IF NOT EXISTS twitter_analysis.good as
SELECT time, count as good
FROM `hardy-symbol-252200.twitter_analysis.wordcount`
WHERE word="good";

CREATE VIEW IF NOT EXISTS twitter_analysis.movie as
SELECT time, count as movie
FROM `hardy-symbol-252200.twitter_analysis.wordcount`
WHERE word="movie";

CREATE VIEW  IF NOT EXISTS twitter_analysis.spark as
SELECT time, count as spark
FROM `hardy-symbol-252200.twitter_analysis.wordcount`
WHERE word="spark";

CREATE TABLE IF NOT EXISTS twitter_analysis.rstcnt AS
(SELECT COALESCE(t1.time, t2.time) as time, IFNULL(data, 0) as data,
IFNULL(ai, 0) as ai, IFNULL(good, 0) as good, IFNULL(movie, 0) as movie,
IFNULL(spark, 0) as spark
FROM
(SELECT COALESCE(t1.time, t2.time) as time, IFNULL(data, 0) as data,
IFNULL(ai, 0) as ai, IFNULL(good, 0) as good, IFNULL(movie, 0) as movie
FROM
(SELECT COALESCE(t1.time, t2.time) as time, IFNULL(data, 0) as data,
IFNULL(ai, 0) as ai
FROM twitter_analysis.data t1
FULL OUTER JOIN
twitter_analysis.ai t2
ON t1.time = t2.time)  t1
FULL OUTER JOIN
(SELECT COALESCE(t1.time, t2.time) as time, IFNULL(good, 0) as good,
IFNULL(movie, 0) as movie
FROM twitter_analysis.good t1
FULL OUTER JOIN
twitter_analysis.movie t2
ON t1.time = t2.time) t2
ON t1.time = t2.time) t1
FULL OUTER JOIN
twitter_analysis.spark t2
ON t1.time = t2.time);


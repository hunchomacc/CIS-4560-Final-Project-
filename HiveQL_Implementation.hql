--create tmp direcroty and add datra file
hdfs dfs -mkdir -p /tmp/robert
hdfs dfs -put complaints_1.csv /tmp/robert/

--create complaints table 
DROP TABLE IF EXISTS complaints;
CREATE EXTERNAL TABLE complaints (
    date_received       DATE,
    product             STRING,
    issue               STRING,
    clean_narrative     STRING,
    zip_code            STRING,
    complaint_id        BIGINT,
    date_clean          DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/robert'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

SELECT * FROM complaints LIMIT 10;

--create trigram_tokens table 
DROP TABLE IF EXISTS trigram_tokens;
CREATE TABLE trigram_tokens AS
SELECT
    complaint_id,
    pos,
    token
FROM complaints
LATERAL VIEW posexplode(split(clean_narrative, ' ')) t AS pos, token
WHERE complaint_id IS NOT NULL
  AND clean_narrative IS NOT NULL
  AND clean_narrative != '';

-- View a sample
SELECT * FROM trigram_tokens
ORDER BY complaint_id, pos
LIMIT 20;


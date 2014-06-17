----------------------------
----------------------------
-- tf-udf
-- Takes the tf example and cleans the input
-- using an UDF
----------------------------
----------------------------

%DEFAULT input 'helloWorld-3.txt'

REGISTER /tmp/uhug.jar
DEFINE CLEANER org.uhug.code.sample.pig.udf.matt.SimpleWordCleanerDB();

----------------------------
-- Load the data 
----------------------------
data = LOAD '$input' using PigStorage() as (txt:chararray);

----------------------------
-- Tokenize data 
----------------------------
tokens = foreach data GENERATE FLATTEN(CLEANER(TOKENIZE(txt, ' '))) as tkn;

----------------------------
-- Group by the token
----------------------------
grouped_tokens = GROUP tokens BY tkn;

----------------------------
-- Count the tokens
----------------------------
counts = FOREACH grouped_tokens GENERATE group as key, COUNT_STAR(tokens) as cnt;

----------------------------
-- Count all the terms
----------------------------
projected_counts = FOREACH counts GENERATE cnt;
grouped_counts = GROUP projected_counts ALL;
sum_counts = FOREACH grouped_counts GENERATE SUM(projected_counts);

----------------------------
-- Generate the TF
----------------------------
tf = FOREACH counts generate key, cnt, (double)cnt / sum_counts.$0 as tf;

----------------------------
-- Put some order in the results
----------------------------
ordered_counts = ORDER tf BY key asc, tf desc;

----------------------------
-- Dump the output
----------------------------
dump ordered_counts;


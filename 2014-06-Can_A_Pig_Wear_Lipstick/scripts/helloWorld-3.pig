----------------------------
----------------------------
-- Hello World
-- Matt Davies
--
-- 3. TFIDF
----------------------------
----------------------------

%DEFAULT input 'helloWorld-3.txt'

----------------------------
-- Load the data 
----------------------------
data = LOAD '$input' using PigStorage() as (txt:chararray);

----------------------------
-- Tokenize data 
----------------------------
tokens = foreach data GENERATE FLATTEN(TOKENIZE(txt, ' ')) as tkn;

----------------------------
-- Group by the token
----------------------------
grouped_tokens = GROUP tokens BY tkn;

----------------------------
-- Count the tokens
----------------------------
counts = FOREACH grouped_tokens GENERATE group as key, COUNT_STAR(tokens) as cnt;

----------------------------
-- Put some order in the results
----------------------------
ordered_counts = ORDER counts BY cnt desc, key asc;

----------------------------
-- Dump the output
----------------------------
dump ordered_counts;

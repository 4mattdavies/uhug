----------------------------
----------------------------
-- Hello World
-- Matt Davies
--
-- 2. TOKENIZE
----------------------------
----------------------------

%DEFAULT input 'helloWorld-2.txt'

----------------------------
-- Load the data 
----------------------------
data = LOAD '$input' using PigStorage() as (txt:chararray);

----------------------------
-- Tokenize the data. 
-- you can see what flatten does as well
----------------------------
--tokens = foreach data GENERATE TOKENIZE(txt, ' ');
tokens = foreach data GENERATE FLATTEN(TOKENIZE(txt, ' '));

----------------------------
-- dump the tokens
----------------------------
dump tokens;

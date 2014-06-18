----------------------------
----------------------------
-- Hello World
-- Matt Davies
--
-- 1. STRSPLIT
----------------------------
----------------------------

%DEFAULT input 'helloWorld-1.txt'

----------------------------
-- Load the data and dump
----------------------------
data = LOAD '$input' using PigStorage() as (txt:chararray);
tokens = foreach data GENERATE FLATTEN(STRSPLIT(txt, ' ', 0));
--tokens = foreach data GENERATE FLATTEN(STRSPLIT(txt, ' ', 0));
dump tokens;

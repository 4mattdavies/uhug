----------------------------
----------------------------
-- Hello World
----------------------------
----------------------------

%DEFAULT input 'helloWorld.txt'

----------------------------
-- Load the data and dump
----------------------------
data = LOAD '$input' using PigStorage() as (txt:chararray);
dump data;


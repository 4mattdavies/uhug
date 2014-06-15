----------------------
-- Anti-Patterns Example
-- Matt Davies
--
-- Yep - I know... I've had to do some _good_ to make this work...
----------------------

---------------------------------------------------------------
--Use non-descript variable names
a = LOAD '$file';
b = FILTER a by e =='some value';

---------------------------------------------------------------
-- Override variables in successive lines
a = LOAD '$file';
a = FILTER a BY e == 'some value';
a = LIMIT a 10;
a = FOREACH a GENERATE e;

---------------------------------------------------------------
--Abscence of comments... hard to show here.

---------------------------------------------------------------
-- Mixing camel and snake case
events = LOAD '$file' using PigStorage();
projectedEvents = FOREACH events GENERATE e;
sample_of_events = LIMIT projectedEvents 100;
samplesGroupedByE = GROUP sample_of_events BY e;

---------------------------------------------------------------
-- Cowboy it and not test locally
-- you know who you are...

---------------------------------------------------------------
-- Don't capitalize keywords
-- Most will work, COUNT_STAR will not ;)
a = load '$file';
a = filter a by e == 'some value';
a = limit a 10;
a = foreach a generate e;

---------------------------------------------------------------
-- leave cruft in scripts
a = LOAD '$file';
--a = FILTER a BY e == 'some other value';
--a = LIMIT a 100;
a = FILTER a BY e == 'some value';
a = LIMIT a 10;
--a = FOREACH a GENERATE e,f,g,h,i; -- why do we not need f,g,h,i ?
a = FOREACH a GENERATE e;

---------------------------------------------------------------
-- throw away key data
-- suppose we expect there to be X 
--   fields in the data. What do we 
--   do if the data exceeds this amount?
raw_data_with_col_count = FOREACH raw_data GENERATE COUNT_STAR(TOBAG($0 ..)) as count, TOTUPLE($0 ..) as data;
good_records = FILTER raw_data_with_col_count BY count == $colcnt;
-- what about the bad records?

---------------------------------------------------------------
-- Don't use variables
good_records = FILTER raw_data_with_col_count BY count == 4;

---------------------------------------------------------------
-- Don't use correct data types
a = LOAD 'someFile' using PigStorage() as (id:int, balance:double, transaction_date:bytearray);
b = LOAD 'someFile' using PigStorage() as (a,b,c,d,e);


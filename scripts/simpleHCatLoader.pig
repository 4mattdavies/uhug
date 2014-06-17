
%DEFAULT db 'matt'
%DEFAULT table 'test'
%DEFAULT output '$db.$table'
%DEFAULT input 'test.tsv'
%DEFAULT partition '1'

----------------------------------------------------
-- Cleanup
----------------------------------------------------

----------------------------------------------------
-- Setup
----------------------------------------------------
SET mapred.output.compress true
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec

data = LOAD '$input' USING PigStorage as (a:chararray,b:chararray);

STORE data INTO '$output'  USING org.apache.hcatalog.pig.HCatStorer('part=$partition');


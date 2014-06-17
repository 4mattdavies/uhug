
%DEFAULT db 'matt'
%DEFAULT table 'test'
%DEFAULT input '$db.$table'

----------------------------------------------------
-- Cleanup
----------------------------------------------------

----------------------------------------------------
-- Setup
----------------------------------------------------
SET mapred.output.compress true
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec

data = LOAD '$input' USING org.apache.hcatalog.pig.HCatLoader();
dump data;


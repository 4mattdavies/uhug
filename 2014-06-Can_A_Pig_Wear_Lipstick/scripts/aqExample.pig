----------------------------
----------------------------
-- Air Quality Example
--
-- Data downloaded from http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html 6/17/2014 @ 9:00 Mountain
----------------------------
----------------------------
%DEFAULT delimiter ','
%DEFAULT input 'aqdata/annual_all_2014.csv'

REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar

----------------------------
-- Load the data 
-- schema is "State Code","County Code","Site Num","Parameter Code","POC","Latitude","Longitude","Datum","Parameter Name","Sample Duration","Pollutant Standard","Metric Used","Method Name","Year","Units of Measure","Event Type","Observation Count","Observation Percent","Valid Day Count","Required Day Count","Exceptional Data Count","Null Data Count","Primary Exceedance Count","Secondary Exceedance Count","Certification Indicator","Num Obs Below MDL","Arithmetic Mean","Arithmetic Standard Dev","1st Max Value","1st Max DateTime","2nd Max Value","2nd Max DateTime","3rd Max Value","3rd Max DateTime","4th Max Value","4th Max DateTime","1st Max Non Overlapping Value","1st NO Max DateTime","2nd Max Non Overlapping Value","2nd NO Max DateTime","99th Percentile","98th Percentile","95th Percentile","90th Percentile","75th Percentile","50th Percentile","10th Percentile","Local Site Name","Address","State Name","County Name","City Name","CBSA Name","Date of Last Change"
--
-- sample data "01","073","0023","42101",2,33.553056,-86.815,"WGS84","Carbon monoxide","1 HOUR","CO 1-hour 1971","Obseved hourly values","Multiple Methods Used",2014,"Parts per million","No Events",1926,22,85,365,0,234,0,0,"Certification not required",234,0.241314,0.192192,1.5,"2014-02-04 07:00",1.42,"2014-01-30 07:00",1.3,"2014-01-18 09:00",1.23,"2014-01-25 21:00","","","","",0.9,0.85,0.67,0.5,0.26,0.19,0.08,"North Birmingham","NO. B'HAM,SOU R.R., 3009 28TH ST. NO.","Alabama","Jefferson","Birmingham","Birmingham-Hoover, AL","2014-05-14"
----------------------------
data = LOAD '$input' using org.apache.pig.piggybank.storage.CSVExcelStorage( '$delimiter', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') as (state_code:int, county_code:int, site_num:int, parameter_code:int, poc:int, latitude:double, longitude:double, datum:chararray, param_name:chararray, sample_duration:chararray, pollutant_standard:chararray, metric_used:chararray, method_name:chararray, year:int, units_of_measure:chararray, event_type:chararray, observation_count:int, observation_percent:double, valid_day_count:int, required_day_count:int, exceptional_data_count:int, null_data_count:int, primary_exceedance_count:int, secondary_exceedance_count:int, certification_indicator:chararray, num_obs_below_mdl:int, arithmetic_mean:double, arithmetic_std_dev:double, first_max_value:double, first_max_date:chararray, second_max_value:double, second_max_date:chararray, third_max_value:double, third_max_date:chararray, fourth_max_value:double, fourth_max_date:chararray, first_max_non_overlapping_value:chararray, first_no_max_date:chararray, second_max_non_overlapping_value:chararray, second_max_non_overlapping_date:chararray, p99:double, p98:double, p95:double, p90:double, p75:double, p50:double, p10:double, local_site_name:chararray, address:chararray, state_name:chararray, county_name:chararray, city_name:chararray, cbsa_name:chararray, date_of_last_change:chararray);
describe data;

----------------------------
-- Get Utah data 
----------------------------
utah_data = FILTER data BY state_name=='Utah';

----------------------------
-- Let's find the outdoor temperature
----------------------------
outdoor_temps = FILTER utah_data BY param_name=='Outdoor Temperature';
projected_outdoor_temp = FOREACH outdoor_temps GENERATE arithmetic_mean, first_max_value, first_max_date, p99, p50, p10, address, state_name;
--dump projected_outdoor_temp;


----------------------------
-- Let's see what is in Syracuse Site num 6002
----------------------------
syracuse_data = FILTER data by site_num==6002;
dump syracuse_data;

----------------------------
----------------------------
-- Lipstick Example
-- Matt Davies
--
-- Data downloaded from http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html 6/17/2014 @ 9:00 Mountain
--
-- Inspired by Alton's presentation : https://www.youtube.com/watch?v=ynE9Y9Zl6ZA
----------------------------
----------------------------
%DEFAULT delimiter ','
%DEFAULT input 'aqdata/utah_daily_pm_2_5_2013.csv'
%DEFAULT date_format 'yyyy-MM-dd'
%DEFAULT output /tmp/outfile

REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar


---------------------------
-- Cleanup
---------------------------
rmf $output

----------------------------
-- Load the data 
--schema "State Code","County Code","Site Num","Parameter Code","POC","Latitude","Longitude","Datum","Parameter Name","Sample Duration","Pollutant Standard","Date Local","Units of Measure","Event Type","Observation Count","Observation Percent","Arithmetic Mean","1st Max Value","1st Max Hour","AQI","Method Name","Local Site Name","Address","State Name","County Name","City Name","CBSA Name","Date of Last Change"
--
-- sample data "01","003","0010","88101",1,30.498001,-87.881412,"NAD83","PM2.5 - Local Conditions","24 HOUR","PM25 24-hour 2006","2013-01-01","Micrograms/cubic meter (LC)","None",1,100.0,7.3,7.3,0,30,"R & P Model 2025 PM2.5 Sequential w/WINS - GRAVIMETRIC","FAIRHOPE, Alabama","FAIRHOPE HIGH SCHOOL, FAIRHOPE,  ALABAMA","Alabama","Baldwin","Fairhope","Daphne-Fairhope-Foley, AL","2014-02-11"
--
----------------------------
data = LOAD '$input' using org.apache.pig.piggybank.storage.CSVExcelStorage( '$delimiter', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') as (state_code:int, county_code:int, site_num:int, parameter_code:int, poc:int, latitude:double, longitude:double, datum:chararray, param_name:chararray, sample_duration:chararray, pollutant_standard:chararray, date_local:datetime, units_of_measure:chararray, event_type:chararray, observation_count:int, observation_percent:double, arithmetic_mean:double, first_max_value:double, fist_max_hour:int, aqi:int, method_name:chararray, local_site_name:chararray, address:chararray, state_name:chararray, county_namer:chararray, city_name:chararray, cbsa_name:chararray, date_last_change:datetime);

----------------------------
-- Generate list of cities
----------------------------
projected_cities = FOREACH data generate city_name;
cities = DISTINCT projected_cities;
--dump cities;

----------------------------
--slim the data
----------------------------
slimmed_data = FOREACH data generate site_num, date_local, arithmetic_mean, observation_count, pollutant_standard, city_name;

----------------------------
-- Split the data
-- notice lazy evaluation
----------------------------
SPLIT slimmed_data INTO logan_data if (city_name=='Logan'), magna_data if (city_name=='Magna'), ogden_data if (city_name=='Ogden'), provo_data if (city_name=='Provo'), slc_data if (city_name=='Salt Lake City');

----------------------------
--Perform some logic to spice things up
----------------------------
o_logan_data = ORDER logan_data by arithmetic_mean;
o_magna_data = ORDER magna_data by arithmetic_mean;

----------------------------
--Monkey with the magna data to make the graph look cool
----------------------------
magna_data = FOREACH o_magna_data generate site_num, date_local, arithmetic_mean;
magna_data2 = FOREACH o_magna_data generate site_num, date_local, observation_count, pollutant_standard, city_name;

----------------------------
-- Join the 2 magna datasets together
-- project the relvant fields
----------------------------
joined_magna = JOIN magna_data BY date_local, magna_data2 BY date_local;
p_magna = FOREACH joined_magna GENERATE magna_data::site_num as site_num, magna_data::date_local as date_local, magna_data::arithmetic_mean as mean, magna_data2::observation_count as observation_count, magna_data2::pollutant_standard as pollutant_standard, magna_data2::city_name;

----------------------------
-- Union the datasets
----------------------------
all_data = UNION o_logan_data, o_magna_data, p_magna;

----------------------------
-- store the data
----------------------------
STORE all_data INTO '$output' using PigStorage();

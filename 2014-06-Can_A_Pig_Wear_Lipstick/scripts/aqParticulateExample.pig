----------------------------
----------------------------
-- AQ Particulate Example
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

REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar
REGISTER /tmp/uhug.jar

DEFINE PRETTY_TIME  org.uhug.code.sample.pig.udf.matt.PrettyTime('$date_format');
DEFINE CHART_VISUALIZER  org.uhug.code.sample.pig.udf.matt.ChartVisualizer();

----------------------------
-- Load the data 
--schema "State Code","County Code","Site Num","Parameter Code","POC","Latitude","Longitude","Datum","Parameter Name","Sample Duration","Pollutant Standard","Date Local","Units of Measure","Event Type","Observation Count","Observation Percent","Arithmetic Mean","1st Max Value","1st Max Hour","AQI","Method Name","Local Site Name","Address","State Name","County Name","City Name","CBSA Name","Date of Last Change"
--
-- sample data "01","003","0010","88101",1,30.498001,-87.881412,"NAD83","PM2.5 - Local Conditions","24 HOUR","PM25 24-hour 2006","2013-01-01","Micrograms/cubic meter (LC)","None",1,100.0,7.3,7.3,0,30,"R & P Model 2025 PM2.5 Sequential w/WINS - GRAVIMETRIC","FAIRHOPE, Alabama","FAIRHOPE HIGH SCHOOL, FAIRHOPE,  ALABAMA","Alabama","Baldwin","Fairhope","Daphne-Fairhope-Foley, AL","2014-02-11"
--
----------------------------
data = LOAD '$input' using org.apache.pig.piggybank.storage.CSVExcelStorage( '$delimiter', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') as (state_code:int, county_code:int, site_num:int, parameter_code:int, poc:int, latitude:double, longitude:double, datum:chararray, param_name:chararray, sample_duration:chararray, pollutant_standard:chararray, date_local:datetime, units_of_measure:chararray, event_type:chararray, observation_count:int, observation_percent:double, arithmetic_mean:double, first_max_value:double, fist_max_hour:int, aqi:int, method_name:chararray, local_site_name:chararray, address:chararray, state_name:chararray, county_namer:chararray, city_name:chararray, cbsa_name:chararray, date_last_change:datetime);

----------------------------
-- Print the date 
----------------------------
small_data = LIMIT data 10;
dates = FOREACH small_data GENERATE PRETTY_TIME(date_local);
--dump dates;

----------------------------
-- Find the arithmetic means for the cities
----------------------------
city_means = FOREACH data GENERATE city_name, arithmetic_mean;
grouped_city_means = GROUP city_means BY city_name;
slimmed_city_means = FOREACH grouped_city_means GENERATE group as city_name, $1.arithmetic_mean as means;
--dump slimmed_city_means;

----------------------------
-- Find min, max, avg, means for the cities
----------------------------
summary_for_cities = FOREACH slimmed_city_means GENERATE city_name, MIN(means.arithmetic_mean) as min, MAX(means.arithmetic_mean) as max, AVG(means.arithmetic_mean) as avg, SUM(means.arithmetic_mean) as sum, COUNT(means.arithmetic_mean) as cnt;
--dump summary_for_cities; 

----------------------------
-- Find the values for just SLC and order by date
----------------------------
slc_summary_data = FILTER summary_for_cities BY city_name=='Salt Lake City';
salt_lake_data = FILTER data BY city_name=='Salt Lake City';
projected_slc_data = FOREACH salt_lake_data GENERATE CHART_VISUALIZER(arithmetic_mean, slc_summary_data.max),ToMilliSeconds(date_local) as millis, FLATTEN(PRETTY_TIME(date_local)) as dt,local_site_name,  arithmetic_mean as mean;
ordered_slc_data = ORDER projected_slc_data BY millis ASC, local_site_name;
--dump ordered_slc_data;

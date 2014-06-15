----------------------------
----------------------------
-- AQ Wind Example
-- Matt Davies
--
-- Data downloaded from http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html 6/17/2014 @ 9:00 Mountain
----------------------------
----------------------------
%DEFAULT delimiter ','
%DEFAULT input 'aqdata/utah_hourly_wind_2014.csv'

REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar

----------------------------
-- Load the data 
-- schema "State Code","County Code","Site Num","Parameter Code","POC","Latitude","Longitude","Datum","Parameter Name","Date Local","Time Local","Date GMT","Time GMT","Sample Measurement","Units of Measure","MDL","Uncertainty","Qualifier","Method Type","Method Name","State Name","County Name","Date of Last Change"
--
--sample data "01","073","0023","61103",1,33.553056,-86.815,"WGS84","Wind Speed - Resultant","2014-01-01","00:00","2013-12-31","18:00",0.5,"Knots",0.1,"","","Non-FRM","Instrumental - Met One Sonic Anemometer Model 50.5","Alabama","Jefferson","2014-03-19"
--
----------------------------
data = LOAD '$input' using org.apache.pig.piggybank.storage.CSVExcelStorage( '$delimiter', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') as (state_code:int, county_code:int, site_num:int, parameter_code:int, poc:int, latitude:double, longitude:double, datum:chararray, param_name:chararray, date_local:chararray, time_local:chararray, date_gmt:chararray, time_gmt:chararray, sample_measurement:double, units_of_measure:chararray, mdl:double, uncertainty:chararray, qualifier:chararray, method_Type:chararray, method_name:chararray, state_name:chararray, county_name:chararray, date_of_last_change:chararray);

----------------------------
-- Let's find the wind readings for Washington County, March, 2014
-- Washington county is site_num 130
-- shows direction and speed
----------------------------
march_data = FILTER data BY site_num==130 AND SUBSTRING(date_local, 0, 7)=='2014-03';

----------------------------
-- What if we wanted to look at the data side by side since it is rolled up hourly?
-- speed code is 61103
-- direction data is 61104
----------------------------
small_projection = FOREACH march_data generate parameter_code, CONCAT(date_local, CONCAT('-',time_local)) as date_time, latitude, longitude, sample_measurement, units_of_measure;
speed_data = FILTER small_projection by parameter_code==61103;
direction_data = FILTER small_projection by parameter_code==61104;
joined_speed_and_direction_data = JOIN speed_data BY date_time FULL OUTER, direction_data BY date_time;
--joined_speed_and_direction_data: {speed_data::parameter_code: int,speed_data::date_time: chararray,speed_data::latitude: double,speed_data::longitude: double,speed_data::sample_measurement: double,speed_data::units_of_measure: chararray,direction_data::parameter_code: int,direction_data::date_time: chararray,direction_data::latitude: double,direction_data::longitude: double,direction_data::sample_measurement: double,direction_data::units_of_measure: chararray}
output_data = FOREACH joined_speed_and_direction_data GENERATE speed_data::date_time as date_time, speed_data::latitude as latitude, speed_data::longitude as longitude, speed_data::sample_measurement as speed, speed_data::units_of_measure as sduom, direction_data::sample_measurement as direction, direction_data::units_of_measure as dduom;

--explain output_data;
dump output_data;

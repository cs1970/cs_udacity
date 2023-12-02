#010 EVENTS TABLES WITH TIMESTAMP DATES 
#010.01 CHECK THE DATE RANGES AVAILABLE ON EACH TABLE (MIN) + (MAX) for all of the EA stations
#ALSO THE COUNT OF DISTINCT DAYS WEHERE DATA WAS AVAILABLE, USE THIS TO ORDER BY
#THIS WILL HIGHLIGHT IF ANY STATION IS DEFICIENT OF DATA

ea_station_data_availability= """
select BBB.town_name, BBB.station_type, BBB.label, BBB.rivername, AAA.* 
from
(select
stationreference
,min(date(reading_date_time)) as min_reading_date
,max(date(reading_date_time)) as max_reading_date
,count(distinct(date(reading_date_time))) as reading_date_count
from ea_all_events
group by stationreference) AAA
left join ea_all_stations BBB 
on AAA.stationreference = BBB.stationreference  
order by AAA.reading_date_count
"""


# 010.02 CHECK THE DATE RANGE OVERLAP ACROSS EA, MET OFFICE AND METEO TABLES
# THE MINIMUM AND MAXIMUM DATE WHERE DATA IS AVAILABLE ACROSS ALL THREE SOURCES
# AND THE COUNT OF DAYS WHERE THERE ARE COMMON VALUES
# WE DO THIS AT TOWN NAME LEVEL
combined_town_data_availability= """
select 
 EEE.town_name               
,min(EEE.reading_date) as min_reading_date
,max(EEE.reading_date) as max_reading_date
,count(distinct EEE.reading_date) as reading_date_count
FROM
(                
SELECT 
coalesce(CCC.town_name, DDD.town_name) as town_name
,coalesce(CCC.reading_date, DDD.reading_date) as reading_date
,coalesce(CCC.EA_IND,0) as EA_IND
,coalesce(CCC.MOFF_IND,0) as MOFF_IND
,coalesce(DDD.METEO_IND,0) as METEO_IND
FROM                  
(                
SELECT 
coalesce(AAA.town_name, BBB.town_name) as town_name
,coalesce(AAA.reading_date, BBB.reading_date) as reading_date
,coalesce(AAA.EA_IND,0) as EA_IND
,coalesce(BBB.MOFF_IND,0) as MOFF_IND                
FROM                                                          
(select distinct town_name, (date(reading_date_time)) as reading_date, 1 as EA_IND from ea_all_events) AAA
full outer join                               
(select distinct town_name, (date(reading_date_time)) as reading_date, 1 as MOFF_IND from met_office_all_events) BBB                                 
on AAA.town_name = BBB.town_name and AAA.reading_date = BBB.reading_date
) CCC                             
full outer join                             
(select distinct town_name, (date(reading_date_time)) as reading_date, 1 AS METEO_IND from meteo_weather_hourly) DDD                                                   
on CCC.town_name = DDD.town_name and CCC.reading_date = DDD.reading_date                                                                         
) EEE
where EEE.EA_IND = 1 and EEE.MOFF_IND = 1 and EEE.METEO_IND = 1
group by EEE.town_name
"""




# 010.02 CHESTERFIELD FLOOD EVENT RELATIONSHIPS BETWEEN RIVER LEVEL AND RAINFALL

# select one river level station in Chesterfield and get hourly readings
chesterfield_ea_river_level= """
select 
max(stationreference) as stationreference
,date_trunc('HOUR',reading_date_time)
,min(value) as min_river_level
,max(value) as max_river_level
,avg(value) as avg_river_level
from ea_all_events where stationreference = 'L0207'
and date(reading_date_time) between '2023-10-20' and '2023-10-22'
group by date_trunc('HOUR',reading_date_time)
order by date_trunc('HOUR',reading_date_time)
"""

# get all of the Chesterfield town related rainfall measuring stations
chesterfield_ea_rainfall= """
select 
stationreference
,date_trunc('HOUR',reading_date_time)
,sum(value) as hourly_rainfall_total
,max(hourly_value) as hourlyinfall_total_x
from ea_all_events where stationreference 
in (select distinct stationreference from ea_all_stations where town_name = 'Chesterfield' and station_type = 'rainfall')
and date(reading_date_time) between '2023-10-20' and '2023-10-20'
group by stationreference, date_trunc('HOUR',reading_date_time)
order by stationreference, date_trunc('HOUR',reading_date_time)
"""



# get rainfall data across all three sources when readings are available
combined_rainfall= """
select
 AAA.town_name
,EEE.county
,AAA.stationreference as EA_STATION_REFERENCE
,DDD.label as EA_STATION_LABEL
,EEE.station_id as MET_OFFICE_STATION_REFERENCE

,round(3956 * 2 * ASIN(
          SQRT( POWER(SIN((DDD.latitude - abs(EEE.latitude)) * pi()/180 / 2), 2) 
              + COS(DDD.longitude * pi()/180 ) * COS(abs(EEE.latitude) * pi()/180)  
              * POWER(SIN((DDD.longitude - EEE.Longitude) * pi()/180 / 2), 2) )),1) as EA_STATION_DIST_FROM_TOWN_MILES

,AAA.reading_date
,AAA.EA_DAILY_RAINFALL_MM
,BBB.MET_OFFICE_DAILY_RAINFALL_MM
,CCC.METEO_DAILY_PRECIPITATION_MM
,CCC.METEO_DAILY_RAINFALL_MM
from 

(select town_name, stationreference, date(reading_date_time) AS reading_date, sum(value) AS EA_DAILY_RAINFALL_MM
from ea_all_events 
where parameter = 'rainfall' 
group by town_name, stationreference, date(reading_date_time)) AAA

inner join                                         
(select town_name, date(reading_date_time) AS reading_date, sum(prcp_amt) AS MET_OFFICE_DAILY_RAINFALL_MM
from met_office_all_events  
group by town_name, date(reading_date_time)) BBB                                                                          
on AAA.town_name = BBB.town_name and AAA.reading_date = BBB.reading_date

inner join
(select town_name, date(reading_date_time) AS reading_date, sum(precipitation) as METEO_DAILY_PRECIPITATION_MM, sum(rain) as METEO_DAILY_RAINFALL_MM
from meteo_weather_hourly  
group by town_name, date(reading_date_time)) CCC                                                 
on AAA.town_name = CCC.town_name and AAA.reading_date = CCC.reading_date

left outer join
ea_all_stations DDD
on AAA.stationreference = DDD.stationreference

left outer join
towns EEE
on AAA.town_name = EEE.town_name
order by AAA.town_name ,AAA.reading_date
"""
list_staging_tables=[
"EA_RIVER_LEVEL_EVENTS",
"EA_RAINFALL_EVENTS",
"EA_RIVER_LEVEL_STATIONS",
"EA_RAINFALL_STATIONS",
"MET_OFFICE_RAINFALL",
"MET_OFFICE_WEATHER",
"METEO_WEATHER_HOURLY",
"METEO_WEATHER_DAILY",
"TOWNS"]

list_dwh_tables=[
"METEO_WEATHER_HOURLY",
"METEO_WEATHER_DAILY",
"TOWNS",
"EA_ALL_STATIONS",
"MET_OFFICE_ALL_EVENTS",
"EA_ALL_EVENTS"
]


# DROP TABLES SQL - generate the sql which is generic for all tables so we can creare in loops

for x in list_staging_tables:
    globals()[f'staging_{x}_drop'] = f'DROP TABLE IF EXISTS staging_{x};'
    
for x in list_dwh_tables:
    globals()[f'{x}_drop'] = f'DROP TABLE IF EXISTS {x} CASCADE;'

    
# CREATE TABLES SQL - each one is bespoke

staging_EA_RIVER_LEVEL_EVENTS_create= """
CREATE TABLE "staging_EA_RIVER_LEVEL_EVENTS" (
"town_name" text,        
"reading_date_time" timestamp,  
"stationReference" text,       
"parameter" text,       
"unitName" text,          
"value" decimal(10,3)     
);
"""

staging_EA_RAINFALL_EVENTS_create= """
CREATE TABLE "staging_EA_RAINFALL_EVENTS" (
"town_name" text,        
"reading_date_time" text,  
"stationReference" text,       
"parameter" text,       
"unitName" text,          
"value" decimal(10,3),
"hourly_value" decimal(10,3)   
);
"""

staging_EA_RIVER_LEVEL_STATIONS_create= """
CREATE TABLE "staging_EA_RIVER_LEVEL_STATIONS" (
"label" text,
"latitude" decimal(12,6),          
"longitude" decimal(12,6),          
"riverName" text,          
"stationReference" text,    
"town" text,        
"town_name" text
);
"""

staging_EA_RAINFALL_STATIONS_create= """
CREATE TABLE "staging_EA_RAINFALL_STATIONS" (
"label" text,
"latitude" decimal(12,6),          
"longitude" decimal(12,6),                   
"stationReference" text,
"town_name" text
);
"""

staging_MET_OFFICE_RAINFALL_create= """
CREATE TABLE "staging_MET_OFFICE_RAINFALL" (
"reading_date_time" timestamp,  
"prcp_amt" decimal(10,3),
"town_name" text
);
"""

staging_MET_OFFICE_WEATHER_create= """
CREATE TABLE "staging_MET_OFFICE_WEATHER" (
"reading_date_time" timestamp,
"wind_direction" decimal(10,3),       
"wind_speed" decimal(10,3),       
"visibility" decimal(10,3),       
"msl_pressure" decimal(10,3),       
"air_temperature" decimal(10,3),       
"rltv_hum" decimal(10,3),
"town_name" text
);
"""

staging_METEO_WEATHER_HOURLY_create= """
CREATE TABLE "staging_METEO_WEATHER_HOURLY" (
"reading_date_time" timestamp,
"temperature_2m" decimal(10,3),       
"precipitation" decimal(10,3),       
"rain" decimal(10,3),       
"wind_speed_10m" decimal(10,3),       
"wind_direction_10m" decimal(10,3), 
"town_name" text
);
"""

staging_METEO_WEATHER_DAILY_create= """
CREATE TABLE "staging_METEO_WEATHER_DAILY" (
"reading_date" text,
"precipitation_sum" decimal(10,3),       
"rain_sum" decimal(10,3),       
"town_name" text   
);
"""

staging_TOWNS_create= """
CREATE TABLE "staging_TOWNS" (
"town_name" text, 
"latitude" decimal(10,6),
"longitude" decimal(10,6),
"level_box_lat_max" decimal(10,6),
"level_box_long_max" decimal(10,6),
"level_box_lat_min" decimal(10,6),
"level_box_long_min" decimal(10,6),
"rainfall_box_lat_max" decimal(10,6),
"rainfall_box_long_max" decimal(10,6),
"rainfall_box_lat_min" decimal(10,6),
"rainfall_box_long_min" decimal(10,6),
"county" text,
"station_id" text
);
"""


# BRING THE EA RIVER LEVEL AND RAINFALL STATIONS TOGETHER AS A SINGLE DIMENSION TABLE
EA_ALL_STATIONS_create= """
CREATE TABLE "EA_ALL_STATIONS" (
"town_name" text,
"station_type" text,
"label" text,
"latitude" decimal(12,6),          
"longitude" decimal(12,6),
"stationReference" text, 
"town" text, 
"riverName" text, 
PRIMARY KEY (stationReference)
);
"""

MET_OFFICE_ALL_EVENTS_create= """
CREATE TABLE "MET_OFFICE_ALL_EVENTS" (
"town_name" text,
"reading_date_time" timestamp,  
"prcp_amt" decimal(10,3),
"wind_direction" decimal(10,3),       
"wind_speed" decimal(10,3),       
"visibility" decimal(10,3),       
"msl_pressure" decimal(10,3),       
"air_temperature" decimal(10,3),       
"rltv_hum" decimal(10,3),
PRIMARY KEY (town_name, reading_date_time)
);
"""

METEO_WEATHER_HOURLY_create= """
CREATE TABLE "METEO_WEATHER_HOURLY" (
"reading_date_time" timestamp,
"temperature_2m" decimal(10,3),       
"precipitation" decimal(10,3),       
"rain" decimal(10,3),       
"wind_speed_10m" decimal(10,3),       
"wind_direction_10m" decimal(10,3), 
"town_name" text,
PRIMARY KEY (town_name, reading_date_time)
);
"""

METEO_WEATHER_DAILY_create= """
CREATE TABLE "METEO_WEATHER_DAILY" (
"reading_date" text,
"precipitation_sum" decimal(10,3),       
"rain_sum" decimal(10,3),       
"town_name" text,
PRIMARY KEY (town_name, reading_date)
);
"""

TOWNS_create= """
CREATE TABLE "TOWNS" (
"town_name" text, 
"latitude" decimal(10,6),
"longitude" decimal(10,6),
"level_box_lat_max" decimal(10,6),
"level_box_long_max" decimal(10,6),
"level_box_lat_min" decimal(10,6),
"level_box_long_min" decimal(10,6),
"rainfall_box_lat_max" decimal(10,6),
"rainfall_box_long_max" decimal(10,6),
"rainfall_box_lat_min" decimal(10,6),
"rainfall_box_long_min" decimal(10,6),
"county" text,
"station_id" text,
PRIMARY KEY (town_name)
);
"""

# BRING THE EA RIVER LEVEL AND RAINFALL TOGETHER AS A SINGLE FACT TABLE
EA_ALL_EVENTS_create= """
CREATE TABLE "EA_ALL_EVENTS" (
"town_name" text,        
"reading_date_time" timestamp,  
"stationReference" text,       
"parameter" text,       
"unitName" text,          
"value" decimal(10,3),
"hourly_value" decimal(10,3),
PRIMARY KEY (stationReference, reading_date_time),
FOREIGN KEY (stationReference) references EA_ALL_STATIONS (stationReference),
FOREIGN KEY (town_name,reading_date_time) REFERENCES MET_OFFICE_ALL_EVENTS (town_name,reading_date_time),
FOREIGN KEY (town_name,reading_date_time) REFERENCES METEO_WEATHER_HOURLY (town_name,reading_date_time),
FOREIGN KEY (town_name,reading_date_time) REFERENCES METEO_WEATHER_DAILY (town_name,reading_date),
FOREIGN KEY (town_name) REFERENCES TOWNS (town_name)
);
"""


# DROP AND CREATE TABLE LISTS OF SQL THAT WILL THEN BE EXECUTED ON REDSHIFT SEQUENTIALLY 
# BY THE CALLING create_tables.py

drop_table_queries = []

for x in list_staging_tables:
    y = globals()[f'staging_{x}_drop']
    drop_table_queries.append(y)
    
for x in list_dwh_tables:
    y = globals()[f'{x}_drop']
    drop_table_queries.append(y)
    
# print("SQL FOR TABLES TO BE DROPPED: ") 
# print(drop_table_queries)

create_table_queries = []

for x in list_staging_tables:
    y = globals()[f'staging_{x}_create']
    create_table_queries.append(y)

for x in list_dwh_tables:
    y = globals()[f'{x}_create']
    create_table_queries.append(y)

# print(create_table_queries)


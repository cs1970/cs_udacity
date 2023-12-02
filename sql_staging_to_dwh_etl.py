# FACT TABLE "EA_ALL_EVENTS"
# THE CONTENT OF THE TWO TABLES USED TO POPULATE IS MUTUALLY EXCLUSIVE SO A UNION IS BEST APPROACH.
# ONE OF THE SOURCE TABLES HAS ADDIITONAL COLUMNS SO SET THOSE TO NULL ON THE TABLE THAT DOESN'T.
# THE RAINFALL TIMESTAMP HAS COME THROUGH TO STAGING AS A STRING SO NEED TO CAST AS TIMESTAMP TO GET IT TO SAVE
EA_ALL_EVENTS_table_insert = """
    DELETE FROM EA_ALL_EVENTS;

    INSERT INTO EA_ALL_EVENTS (town_name,reading_date_time,stationReference,parameter,unitName,value,hourly_value)
    SELECT town_name,reading_date_time,stationReference,parameter,unitName,value,null as hourly_value FROM staging_EA_RIVER_LEVEL_EVENTS;

    INSERT INTO EA_ALL_EVENTS (town_name,reading_date_time,stationReference,parameter,unitName,value,hourly_value)
    SELECT town_name,cast(reading_date_time as timestamp),stationReference,parameter,unitName,value,hourly_value FROM staging_EA_RAINFALL_EVENTS;
"""

# DIMENSION TABLE "EA_ALL_STATIONS"
# THE CONTENT OF THE TWO TABLES USED TO POPULATE IS MUTUALLY EXCLUSIVE SO A UNION IS BEST APPROACH.
# ONE OF THE SOURCE TABLES HAS ADDIITONAL COLUMNS SO SET THOSE TO NULL ON THE TABLE THAT DOESN'T.
EA_ALL_STATIONS_table_insert = """
    DELETE FROM EA_ALL_STATIONS;

    INSERT INTO EA_ALL_STATIONS (town_name,station_type,label,latitude,longitude,stationReference,town,riverName)
    SELECT town_name,'river level' as station_type,label,latitude,longitude,stationReference,town,riverName FROM staging_EA_RIVER_LEVEL_STATIONS;
    
    INSERT INTO EA_ALL_STATIONS (town_name,station_type,label,latitude,longitude,stationReference,town,riverName)
    SELECT town_name,'rainfall' as station_type,label,latitude,longitude,stationReference, null AS riverName, null AS town FROM staging_EA_RAINFALL_STATIONS;
"""

# DIMENSION TABLE "MET_OFFICE_ALL_EVENTS"
# this involve full outer join across the two met office events tables
MET_OFFICE_ALL_EVENTS_table_insert = """
    DELETE FROM MET_OFFICE_ALL_EVENTS;

    INSERT INTO MET_OFFICE_ALL_EVENTS (town_name,reading_date_time,prcp_amt,wind_direction,wind_speed,visibility,msl_pressure,air_temperature,rltv_hum)
    SELECT 
     COALESCE(r.town_name,w.town_name) AS town_name
    ,COALESCE(r.reading_date_time,w.reading_date_time) AS reading_date_time
    ,r.prcp_amt
    ,w.wind_direction
    ,w.wind_speed
    ,w.visibility
    ,w.msl_pressure
    ,w.air_temperature
    ,w.rltv_hum
    FROM staging_MET_OFFICE_RAINFALL r
    FULL OUTER JOIN staging_MET_OFFICE_WEATHER w
    ON r.town_name = w.town_name
    AND r.reading_date_time = w.reading_date_time;
"""

# DIMENSION TABLE "METEO_WEATHER_DAILY"
METEO_WEATHER_DAILY_table_insert = """
    DELETE FROM METEO_WEATHER_DAILY;
    
    INSERT INTO METEO_WEATHER_DAILY (reading_date,precipitation_sum,rain_sum,town_name)
    SELECT reading_date,precipitation_sum,rain_sum,town_name FROM staging_METEO_WEATHER_DAILY;
"""

# DIMENSION TABLE "METEO_WEATHER_HOURLY"
METEO_WEATHER_HOURLY_table_insert = """
    DELETE FROM METEO_WEATHER_HOURLY;
    
    INSERT INTO METEO_WEATHER_HOURLY (reading_date_time,temperature_2m,precipitation,rain,wind_speed_10m,wind_direction_10m,town_name)
    SELECT reading_date_time,temperature_2m,precipitation,rain,wind_speed_10m,wind_direction_10m,town_name FROM staging_METEO_WEATHER_HOURLY;
"""

# DIMENSION TABLE "TOWNS"
TOWNS_table_insert = """
    DELETE FROM TOWNS;
    
    INSERT INTO TOWNS (town_name,latitude,longitude,level_box_lat_max
    ,level_box_long_max,level_box_lat_min,level_box_long_min
    ,rainfall_box_lat_max,rainfall_box_long_max,rainfall_box_lat_min,rainfall_box_long_min
    ,county,station_id)
    SELECT town_name,latitude,longitude,level_box_lat_max
    ,level_box_long_max,level_box_lat_min,level_box_long_min
    ,rainfall_box_lat_max,rainfall_box_long_max,rainfall_box_lat_min,rainfall_box_long_min
    ,county,station_id FROM staging_TOWNS;
"""

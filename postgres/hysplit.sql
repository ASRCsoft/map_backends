-- make database
create database hysplit_xcite; 

-- make tables
create table sites (id serial primary key, stid varchar unique, number smallint, name varchar, latitude float, longitude float, elevation float, county varchar, nearest_city varchar, state varchar, distance_from_town float, direction_from_town varchar, climate_division smallint, climate_division_name varchar, wfo varchar, commissioned timestamp, decommissioned timestamp);

create table available_times (id serial primary key, time timestamp unique not null, active bool);

create table simulations (id serial primary key, site_id int references sites, time_id int references available_times on delete cascade not null, forward bool not null, metadata jsonb, unique(site_id, time_id, forward));

-- scrapping this in favor of a table that uses height and time
-- indices instead of raw values:
-- create table contours (simulation_id int references simulations on delete cascade, height float, time timestamp, topojson jsonb);

create table contours (simulation_id int references simulations on delete cascade, height smallint, time smallint, topojson jsonb, primary key(simulation_id, height, time));

-- get sites from the mesonet csv
copy sites(stid, number, name, latitude, longitude, elevation, county, nearest_city, state, distance_from_town, direction_from_town, climate_division, climate_division_name, wfo, commissioned, decommissioned) from '/home/xcite/public_html/hysplit_xcite/data/nysm.csv' delimiter ',' csv header;

-- going to use a foreign data wrapper to make it easier to update the
-- sites table -- following advice from
-- http://thebuild.com/blog/2016/01/14/doing-a-bulk-merge-in-postgresql-9-5/
create extension file_fdw;
create server import_csv foreign data wrapper file_fdw;
-- this table uses the nysm.csv file that Jeongran is using
create foreign table nysm_csv (stid varchar, number smallint, name varchar, latitude float, longitude float, elevation float, county varchar, nearest_city varchar, state varchar, distance_from_town float, direction_from_town varchar, climate_division smallint, climate_division_name varchar, wfo varchar, commissioned timestamp, decommissioned timestamp) server import_csv options (filename '/home/xcite/hysplit/nysm.csv', format 'csv', header 'true');

-- now we can update the sites with this (admittedly long and confusing) command:
insert into sites(stid, number, name, latitude, longitude, elevation, county, nearest_city, state, distance_from_town, direction_from_town, climate_division, climate_division_name, wfo, commissioned, decommissioned) select * from nysm_csv on conflict (stid) do update set (number, name, latitude, longitude, elevation, county, nearest_city, state, distance_from_town, direction_from_town, climate_division, climate_division_name, wfo, commissioned, decommissioned) = (select number, name, latitude, longitude, elevation, county, nearest_city, state, distance_from_town, direction_from_town, climate_division, climate_division_name, wfo, commissioned, decommissioned from nysm_csv where nysm_csv.stid=excluded.stid);

-- add data?


-- grant permissions needed to run the hysplit2json script
grant select, insert, update on sites to jyun;
grant usage, select on sequence sites_id_seq to jyun;
grant select, insert, update, delete on available_times to jyun;
grant usage, select on sequence available_times_id_seq to jyun;
grant select, insert, update on simulations to jyun;
grant usage, select on sequence simulations_id_seq to jyun;
grant select, insert, update on contours to jyun;

grant select on nysm_csv to jyun;

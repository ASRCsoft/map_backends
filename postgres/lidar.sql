-- setting up the database and tables for the lidar database


-- make database
create database lidar;


-- add extensions
create extension if not exists timescaledb cascade;

-- Needed for filesfdw, used for searching data files stored on the
-- app server. The required filesfdw python package can be found on
-- the asrcsoft github page. Make sure to install it in the system
-- python so postgres can use it!
create extension multicorn;


-- Common data shared by both lidars and microwave radiometers:

-- sites
create table sites (stid varchar primary key, number smallint, name varchar, latitude float, longitude float, elevation float, county varchar, nearest_city varchar, state varchar, distance_from_town float, direction_from_town varchar, climate_division smallint, climate_division_name varchar, wfo varchar, commissioned timestamp, decommissioned timestamp);
-- add data from the csv file
copy sites from '/home/xcite/lidar_db/NYSM_Profiler_Locations_20170920.csv' delimiter ',' csv header;


-- The main data tables:

-- Lidar data:
-- lidars table: 'name' is the displayed name, stid is 'CESTM_roof-14' etc.
create table lidars (id smallserial primary key, stid varchar unique not null, site varchar references sites, name varchar);

-- scans table
create table scans (id smallserial primary key, lidar_id smallint references lidars not null, xml xml not null);
-- I would like to do something like this:
-- create table scans (id smallserial primary key, lidar_id smallint references lidars not null, xml xml not null, unique(lidar_id, xmlserialize(document xml as varchar)));
create table lidar_configs (id smallserial primary key, lidar_id smallint references lidars not null, xml xml not null);

-- To cut down on the number of rows in the data tables, I'm stuffing
-- all the data associated with each profile into one row. That means
-- ~100 radial wind speed measurements, ~100 CNR measurements, etc. in
-- every row. To do this I'm storing each set of measurements as an
-- array. This cuts down the number of rows that have to be found by a
-- factor of ~100, which makes it much more practical to access the
-- data.

-- Even after cutting it down by a factor of 100, we still have an
-- impractically large number of rows. To help deal with that I'm
-- using timescaledb: https://www.timescale.com/

-- Updating poses another problem. When running update queries, behind
-- the scenes postgres deletes the affected rows and replaces them
-- with totally new rows. That means if I want to update one value in
-- a row all the other values in that row will also be discarded and
-- replaced. Thus to make updates faster it's better to split the data
-- into logical groupings of variables that will typically be updated
-- in common, rather than having all data combined in one common
-- table.

-- profiles (lidar data) table
create table profiles (lidar_id smallint not null references lidars, configuration_id smallint, scan_id smallint not null references scans, sequence_id int, los_id smallint, azimuth real, elevation real, time timestamp not null, cnr real[], rws real[], drws real[], status boolean[], error real[], confidence real[], primary key(lidar_id, time));
-- make profiles a hypertable + set up timescaledb:
select create_hypertable('profiles', 'time', chunk_time_interval => interval '1 day');

-- wind stuff

-- really this could be added to profiles but that would mean every
-- time I want to update wind values I have to update the entire
-- profiles table (horrifying!). So instead it gets its own smaller
-- table that's easier to update
create table wind (lidar_id smallint not null, scan_id smallint not null, time timestamp not null, xwind real[], ywind real[], zwind real[], primary key(lidar_id, time), foreign key(lidar_id, scan_id) references scans(lidar_id, id));
-- make it a hypertable:
select create_hypertable('wind', 'time', chunk_time_interval => '1 day');

-- lidar 5 minute summary data (the default timescaledb interval of 1 month should be fine here)
-- this is used for the 'quick look' tool (profiles page) on the xcite website
create table lidar5m (scan_id smallint not null, time timestamp not null, cnr real[], cnr_whole real[], drws real[], xwind real[], ywind real[], zwind real[], primary key(scan_id, time), foreign key(scan_id) references scans);
select create_hypertable('lidar5m', 'time');

-- lidar 15 minute summary data
-- this is for the CNR gradient for PBL calculations
create table lidar15m (scan_id smallint not null, time timestamp not null, cnr_whole real[], zwind_var real[], zwind_n int[], primary key(scan_id, time), foreign key(scan_id) references scans);
select create_hypertable('lidar15m', 'time');

-- lidar 30 minute summary data
-- also need vertical wind variance for PBL calculations

-- add z wind
create table lidar30m (scan_id smallint not null, time timestamp not null, zwind_var real[], n int[], primary key(scan_id, time), foreign key(scan_id) references scans);
select create_hypertable('lidar30m', 'time');
-- this was for a non-existent wind page (the tke calculations were incorrect)
-- create table lidar30m (scan_id smallint not null, time timestamp not null, tke real[], alpha real[], primary key(scan_id, time), foreign key(scan_id) references scans);



-- Mwr data:
-- mwr's
create table mwrs (id smallserial primary key, stid varchar unique not null, site varchar references sites, name varchar);
-- insert from mwr csv files
insert into mwrs (stid) select distinct site from mwr_csv;
create table mwr_scans (id smallserial primary key, mwr_id smallint references mwrs not null, processor varchar not null);

-- mwr profiles
-- this has the arrays of profile measurements (temperature, vapor
-- pressure, etc) along with their quality values (tempq, vaporq, etc)
create table mwr_profiles2 (mwr_id smallint references mwrs, scan_id smallint not null references mwr_scans, time timestamp, temp float[], vapor float[], liquid float[], rh float[], tempq bool, vaporq bool, liquidq bool, rhq bool, primary key(scan_id, time));
select create_hypertable('mwr_profiles2', 'time');



-- filesfdw tables:
create server lidar_csv_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.LidarCsv');
create foreign table lidar_csv (
  date date,
  site text,
  radial text,
  scan text,
  environment text,
  config text,
  wind text,
  whole text,
  sequences text,
  new_rws text,
  new_wind text,
  new_env text
) server lidar_csv_srv options(base '/xcitemnt/mesonet/data/lidar_raw/');

create server mwr_csv_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.MwrCsv');
create foreign table mwr_csv (
  time timestamp,
  site text,
  lv0 text,
  lv1 text,
  lv2 text,
  tip text,
  healthstatus text
) server mwr_csv_srv options(base '/xcitemnt/mesonet/data/mwr_raw/');

create server lidar_nc_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.LidarNetcdf');
create foreign table lidar_netcdf (
  date date,
  site text,
  netcdf text
) server lidar_nc_srv options(base '/web/html/private/lidar_netcdf/');


-- I really should change this to use multicorn.fsfdw.FilesystemFdw
-- which provides a more standardized interface!
CREATE SERVER filesystem_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.fsfdw.FilesystemFdw'
);

-- try to remake the lidar_csv table
CREATE FOREIGN TABLE lidar_csv0 (
  site text,
  year int,
  month int,
  date date,
  type text,
  ext text,
  content bytea,
  filename text
) server filesystem_srv options(
  root_dir    '/xcitemnt/mesonet/data/lidar_raw/',
  pattern     '{site}/{year}/{month}/{date}_{type}.{ext}',
  content_column 'content',
  filename_column 'filename'
);

-- try to remake the mwr_csv table
CREATE FOREIGN TABLE mwr_csv0 (
  site text,
  year int,
  month int,
  date date,
  time text,
  type text,
  ext text,
  content bytea,
  filename text
) server filesystem_srv options(
  root_dir    '/xcitemnt/mesonet/data/mwr_raw/',
  pattern     '{site}/{year}/{month}/{date}_{time}_{type}.{ext}',
  content_column 'content',
  filename_column 'filename'
);

-- get some info from the listos sonde files
CREATE FOREIGN TABLE listos_csv0 (
    datetime_o3 text,
    location text,
    content bytea,
    filename character varying
) server filesystem_srv options(
    root_dir    '/home/will/OneDrive/Merged Data',
    pattern     '{datetime_o3}-sonde-{location}.txt',
    content_column 'content',
    filename_column 'filename'
);
-- now create a view which transforms the data
create or replace view listos_csv as
select replace(location, '-', ' ') as location,
       to_timestamp(split_part(datetime_o3, '-', 1), 'YYYYMMDDHH24MI') as launch_time,
       split_part(datetime_o3, '-', 2)='o3' as o3,
       content as csv,
       filename as file
from listos_csv0;




-- the csv file with profiler sites
create extension file_fdw;
create server import_csv foreign data wrapper file_fdw;
create foreign table sites_csv (stid varchar, number smallint, name varchar, latitude float, longitude float, elevation float, county varchar, nearest_city varchar, state varchar, distance_from_town float, direction_from_town varchar, climate_division smallint, climate_division_name varchar, wfo varchar, commissioned timestamp, decommissioned timestamp) server import_csv options (filename '/home/xcite/lidar_db/NYSM_Profilers_withElevation.csv', format 'csv', header 'true');





-- radiosonde data!
create schema radiosonde;
create table radiosonde.releases (id serial primary key, file text, version text, station text, flight text, time timestamp);

create table radiosonde.records (release_id int references radiosonde.releases, elapsed_time float, time_stamp timestamp, corrected_pressure float, smoothed_pressure float, geopotential_height int, corrected_temperature float, potential_temperature float, corrected_rh float, dewpoint_temperature float, dewpoint_depression float, mixing_ratio float, ascension_rate float, temperature_lapse float, corrected_azimuth float, corrected_elevation float, wind_direction float, wind_speed float, u float, v float, latitude float, longitude float, geometric_height float, arc_distance float, primary key(release_id, time_stamp));
-- make records a hypertable + set up timescaledb:
select create_hypertable('radiosonde.records', 'time_stamp');


-- listos radiosonde data!
create schema listos_sonde;
create table listos_sonde.releases (id serial primary key, station text unique, launch_time timestamp, file text, version text, flight text, observer text);
create table listos_sonde.records (id serial primary key, release_id int references radiosonde.releases, time_stamp timestamp, elapsed_time float, pressure float, height float, temperature float, teta float, rh float, dewpoint float, mixing_ratio float,  u float, v float, wind_direction float, wind_speed float, latitude float, longitude float, ascent_rate float, o3 float, total_ozone float, res_ozone float, o3_current float, box_temperature float, pump_current float, unique(release_id, time_stamp));

-- just make a table with pbl estimates?
create schema sonde;
create table sonde.pbl (id serial primary key, time timestamp, site_id text references sites(stid), heffter_pbl real, liu_liang_pbl real, richardson_pbl real, unique(time, site_id));

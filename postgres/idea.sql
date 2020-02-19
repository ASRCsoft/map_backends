-- tables to store IDEA simulation results and related data

-- this is going to exist in the dispersion database (currently called
-- hysplit_xcite) under different schemas to keep things organized

create extension postgis;
create extension multicorn;

create schema idea;
create table idea.simulations (id serial primary key, time_range tsrange, trajectories jsonb, high_resolution boolean, unique(time_range, high_resolution));

create schema viirs;
-- create table viirs.swaths (id serial primary key, time timestamp, cod raster, aod raster, high_resolution boolean, unique(time, high_resolution));
create table viirs.swaths (id serial primary key, time timestamp, swath raster, high_resolution boolean, unique(time, high_resolution));

-- NWP model inputs
create table idea.nwp (id serial primary key, time timestamp, nwp raster, high_resolution boolean, unique(time, high_resolution));


-- some filesystem tables to make updating more convenient
create server filesystem_srv foreign data wrapper multicorn options(wrapper 'multicorn.fsfdw.FilesystemFdw');
CREATE FOREIGN TABLE idea_nc0 (
    hr1 text,
    region text,
    hr2 text,
    date1 text,
    hr3 text,
    dataset text,
    duration int,
    date2 text,
    content bytea,
    filename character varying
) server filesystem_srv options(
    root_dir    '/lulab/weiting/IDEA-I',
    pattern     'IDEA-I_aerosol{hr1}/products/{region}/Aerosol{hr2}/SNPP/{date1}/VIIRSaerosol{hr3}S_{dataset}_{duration}hr_{date2}.nc',
    content_column 'content',
    filename_column 'filename');
create or replace view idea_nc as
select to_timestamp(date2, 'YYYYMMDD')::timestamp without time zone::date as date,
       hr1='EntHR' as high_resolution,
       region,
       dataset,
       duration,
       content,
       filename as file
from idea_nc0;

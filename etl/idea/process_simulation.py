# this script takes a set of simulation output netCDF files and
# organizes it into database- and web-friendly data formats


# I should consider storing raster data as half-precision (16bit)
# floats to save space and for easier conversion to png

# import matplotlib.pyplot as plt
import json, datetime, simplejson, glob, re, warnings
import numpy as np
import pandas as pd
import xarray as xr
from converter import make_trajectories, get_raster_binary
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy_utils.types.range import DateTimeRangeType
import psycopg2
from psycopg2.extras import DateTimeRange, Json
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
# for swaths and nwp
from osgeo import gdal, osr
gdal.UseExceptions()

# set up postgres connection
def nan_to_null(data):
    return simplejson.dumps(data, ignore_nan=True)
# pg = sqlalchemy.create_engine('postgresql:///hysplit_xcite',
#                               json_serializer=nan_to_null)
pg = sqlalchemy.create_engine('postgresql:///lidar',
                              json_serializer=nan_to_null)
m1 = sqlalchemy.schema.MetaData(pg, schema="idea")
m1.reflect()

# traj_file = '/lulab/weiting/IDEA-I/IDEA-I_aerosol/products/CONUS/Aerosol/SNPP/20180401/VIIRSaerosolS_traj_48hr_20180401.nc'
# grid_file = 'VIIRSaerosolEntHRS_grid_36hr_20180101.nc'


# Utilities

def get_new_traj_files(pg):
    '''Find IDEA trajectory netCDF files to that haven't been added to the
    the idea.simulations table.'''
    q = "select file, nc.high_resolution from idea_nc nc left join idea.simulations sim on lower(sim.time_range)::date=nc.date and sim.high_resolution=nc.high_resolution where nc.dataset='traj' and sim.id is null"
    new_files = pd.read_sql(q, pg)
    # add the base of the filesystem
    new_files['file'] = '/lulab/weiting/IDEA-I/' + new_files['file']
    return new_files

def get_new_grid_files(pg):
    '''Find IDEA netCDF grid files to that haven't been added to the the
    viirs.swaths or idea.nwp table.'''
    q = "select distinct file, nc.high_resolution from idea_nc nc left join idea.nwp nwp on nwp.time::date=nc.date and nwp.high_resolution=nc.high_resolution left join viirs.swaths swt on swt.time::date=nc.date and swt.high_resolution=nc.high_resolution where nc.dataset='grid' and (nwp.id is null or swt.id is null)"
    # q = "select distinct file, nc.high_resolution from idea_nc nc where nc.dataset='grid' and high_resolution and date>='2018-08-28'"
    new_files = pd.read_sql(q, pg)
    # add the base of the filesystem
    new_files['file'] = '/lulab/weiting/IDEA-I/' + new_files['file']
    return new_files

# def get_nc_files():
#     return glob.glob('/lulab/weiting/IDEA-I/IDEA-I_aerosol/products/CONUS/Aerosol/SNPP/*/VIIRSaerosolS_grid_*.nc')

def npdt_to_dt(time):
    # convert numpy datetime to datetime.datetime
    return pd.to_datetime(str(time)).to_pydatetime()

def get_date_from_nc_file(nc_file):
    '''Get the date from a netCDF file path'''
    date_str = re.sub(r'^.*_|\.nc$', '', nc_file)
    return datetime.datetime.strptime(date_str, '%Y%m%d')



# Trajectories

def add_trajectories_to_pg(pg, traj_file, high_resolution):
    # make trajectory geojson
    traj_ds = xr.open_dataset(traj_file)
    traj_gj = make_trajectories(traj_ds)
    # get simulation time range
    start_time = npdt_to_dt(traj_ds.coords['time'].values[0])
    end_time = npdt_to_dt(traj_ds.coords['time'].values[-1])
    sim_time_range = DateTimeRange(start_time, end_time, '[]')
    # add to postgres
    simulation = {'time_range': sim_time_range,
                  'trajectories': traj_gj,
                  'high_resolution': high_resolution}
    ins1 = insert(m1.tables['idea.simulations'])
    ins1 = ins1.on_conflict_do_update(
        index_elements=['time_range', 'high_resolution'],
        set_=dict(ins1.excluded)
    )
    with pg.connect() as con:
        con.execute(ins1, simulation)



# Swaths

def get_swath_count(ds):
    '''Get the number of swaths in the GDAL dataset'''
    clavrx_count = int(ds.GetMetadata()['NC_GLOBAL#CLAVRX_SWATHS'])
    vaooo_count = int(ds.GetMetadata()['NC_GLOBAL#VAOOO_SWATHS'])
    if clavrx_count != vaooo_count:
        raise Exception("AOD and COD swath counts don't match")
    return clavrx_count

def get_swath_time(ds, swath_id):
    '''Get the swath time from a GDAL dataset'''
    swath_str = 'NC_GLOBAL#VAOOO_SWATH_%03d' % swath_id
    hours_str = ds.GetMetadata()[swath_str]
    hours = float(re.sub(r' .*$', '', hours_str))
    return datetime.timedelta(hours=1) * hours

def get_swath_ds(nc_file, swath_id):
    '''Read AOD and COD from the netCDF file and return it as a multiband
    gdal in-memory dataset

    '''
    aod_str = 'NETCDF:"%s":AerosolOpticalDepth_at_550nm_%03d' % (nc_file, swath_id)
    cod_str = 'NETCDF:"%s":cld_opd_dcomp_%03d' % (nc_file, swath_id)
    aod = gdal.Warp('', aod_str, geoloc=True, format='MEM', dstSRS='EPSG:3857')
    cod = gdal.Warp('', cod_str, geoloc=True, format='MEM', dstSRS='EPSG:3857')
    aod.AddBand(aod.GetRasterBand(1).DataType)
    aod.GetRasterBand(2).SetNoDataValue(-999)
    aod.GetRasterBand(2).WriteArray(cod.GetRasterBand(1).ReadAsArray())
    return aod

def _add_swath_to_pg(conn, swath, swath_time, hr):
    '''Add a VIIRS swath to postgres'''
    cur = conn.cursor()
    cur.execute("insert into viirs.swaths (time, swath, high_resolution) values (%s, %s::raster, %s) on conflict(time, high_resolution) do update set swath=excluded.swath",
                (swath_time, get_raster_binary(swath), hr))
    cur.close()

def add_swath_to_pg(conn, nc_file, swath_id, hr):
    '''Add a VIIRS swath to postgres'''
    swath = get_swath_ds(nc_file, swath_id)
    swath_time = get_date_from_nc_file(nc_file) + get_swath_time(swath, swath_id)
    _add_swath_to_pg(conn, swath, swath_time, hr)
    # close the dataset
    swath = None
    return swath_time

def process_swaths(conn, nc_file, hr):
    '''Add netCDF swaths to postgres'''
    gdal_str = 'NETCDF:"%s"' % nc_file
    ds = gdal.Open(gdal_str)
    swath_count = get_swath_count(ds)
    for swath_id in range(1, swath_count + 1):
        try:
            add_swath_to_pg(conn, nc_file, swath_id, hr)
        except:
            warnings.warn('NetCDF file reported %s swaths, but swath %s failed.' %
                          (swath_count, swath_id))
    ds = None



# WRF outputs

def open_nwp_raster(gdal_str):
    '''Open NWP output raster from a regular IDEA grid file.'''
    r = gdal.Open(gdal_str)
    # fix projection info
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    r.SetProjection(srs.ExportToWkt())
    # and fix the geotransform
    r.SetGeoTransform((-126, .5, 0, 24, 0, .5))
    return r

def open_nwp_hr_raster(gdal_str):
    '''Open NWP output raster from an IDEA-NYS grid file. Since the data
    is provided in an irregular lat/lon grid, this requires 'warping'
    the data to a regular map projection.

    '''
    return gdal.Warp('', gdal_str, geoloc=True, format='MEM')

def get_rasters_from_nc(nc_file, hr):
    '''get the interesting rasters from the netcdf file'''
    if hr:
        ds_names = ['upbl', 'vpbl', 'apcp']
    else:
        p_heights = [500, 700, 850]
        ds_names = ([ 'uwind%s' % h for h in p_heights ] +
                    [ 'vwind%s' % h for h in p_heights ] +
                    ['apcp'])
    gdal_strs = [ 'NETCDF:"%s":%s' % (nc_file, s) for s in ds_names ]
    if hr:
        rasters = list(map(open_nwp_hr_raster, gdal_strs))
    else:
        rasters = list(map(open_nwp_raster, gdal_strs))
    return rasters

def get_nwp(rasters, band, hr):
    '''Reorganize rasters into a combined raster'''
    # create the new raster
    nwp = gdal.Translate('', rasters[0], format='MEM', bandList=[1])
    # add the rest of the wind rasters
    if hr:
        nrasters = 3
    else:
        nrasters = 7
    for i, raster in enumerate(rasters[1:(nrasters - 1)]):
        nwp.AddBand(raster.GetRasterBand(band).DataType)
        nodata = raster.GetRasterBand(band).GetNoDataValue()
        nwp.GetRasterBand(i + 2).SetNoDataValue(nodata)
        nwp.GetRasterBand(i + 2).WriteArray(raster.GetRasterBand(band).ReadAsArray())
    # add the rain raster
    apcp = rasters[nrasters - 1]
    if apcp.RasterCount > band:
        nwp.AddBand(apcp.GetRasterBand(band + 1).DataType)
        nodata = apcp.GetRasterBand(band + 1).GetNoDataValue()
        nwp.GetRasterBand(nrasters).SetNoDataValue(nodata)
        if hr and band % 3 != 1:
            # subtract to find hourly precipitation
            apcp1 = apcp.GetRasterBand(band).ReadAsArray()
            apcp2 = apcp.GetRasterBand(band + 1).ReadAsArray()
            # be careful with missing values
            apcp1[apcp1 == nodata] = np.nan
            apcp2[apcp2 == nodata] = np.nan
            apcp_diff = apcp2 - apcp1
            apcp_diff[np.isnan(apcp_diff)] = nodata
            nwp.GetRasterBand(nrasters).WriteArray(apcp_diff)
        else:
            apcp1 = apcp.GetRasterBand(band + 1).ReadAsArray()
            nwp.GetRasterBand(nrasters).WriteArray(apcp1)
    return nwp

def get_times_from_raster(ds):
    '''Get the times from a GDAL raster.'''
    meta = ds.GetMetadata()
    units = meta['time#units']
    start = datetime.datetime.strptime(units, 'hours since %Y-%m-%d %H:%M:%S %Z')
    tvalues = list(map(int, meta['NETCDF_DIM_time_VALUES'][1:-1].split(',')))
    intervals = np.timedelta64(1, 'h') * np.array(tvalues)
    return np.datetime64(start) + intervals

# def get_times_from_nc(nc_file):
#     '''get the NWP raster times from the netcdf file'''
#     ds = xr.open_dataset(nc_file)
#     times = ds['time'].values
#     ds.close()
#     # don't need to read into xarray -- can get times from
#     # NETCDF_DIM_time_VALUES and time#units attributes available in
#     # any subdataset with the time dimension
#     return times

def add_nwp_to_pg(pg, time, nwp, hr):
    '''Add gridded NWP output to postgres'''
    # --- need to add some method for handling missing precipitation
    # --- data
    # No not true!-- just store precipitation as usual. The newer
    # filled-in data will replace the old missing data
    with pg.cursor() as cur:
        cur.execute("insert into idea.nwp (time, nwp, high_resolution) values (%s, %s::raster, %s) on conflict (time, high_resolution) do update set nwp = excluded.nwp",
                    (npdt_to_dt(time), get_raster_binary(nwp), hr))

def process_nwp(pg, nc_file, hr):
    # 1) get the rasters
    rasters = get_rasters_from_nc(nc_file, hr)
    times = get_times_from_raster(rasters[0])
    nbands = rasters[0].RasterCount
    # 2) add combined rasters to postgres
    for n in range(nbands):
        nwp = get_nwp(rasters, n + 1, hr)
        add_nwp_to_pg(pg, times[n], nwp, hr)
        # close the dataset
        nwp = None




# Main -- update everything


# loop through all the trajectory files
print('Starting trajectories...')
# get the new files
traj_files = get_new_traj_files(pg)
for index, row in traj_files.iterrows():
    traj_file = row['file']
    print('starting %s...' % traj_file)
    add_trajectories_to_pg(pg, traj_file, row['high_resolution'])


# alright, let's add the data already dude
print('Starting swaths and NWP output...')
# grid_files = get_nc_files()
grid_files = get_new_grid_files(pg)
with psycopg2.connect("dbname=lidar user=will") as conn: 
    # autocommit MUST be set to true for the postgres raster commands to work
    conn.autocommit = True
    for index, row in grid_files.iterrows():
    # for grid_file in grid_files:
        grid_file = row['file']
        hr = row['high_resolution']
        print('Starting file for %s' % get_date_from_nc_file(grid_file))
        process_swaths(conn, grid_file, hr)
        process_nwp(conn, grid_file, hr)

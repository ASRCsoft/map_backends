# different version -- this time the trajectories are all contained in a polyline
# (needs a bit of work on the leaflet end)

import json, geojson, sys
import numpy as np
import pandas as pd


# 1) converting trajectories

def fix_times(times):
    '''The IDEA trajectory model records the model start time for the
    first time, even if the trajectory hasn't started yet. This
    function removes the time gap between first time and the remaining
    times (if it exists).

    '''
    if times[1] - times[0] > np.timedelta64(1, 'h'):
        times[0] = times[1] - np.timedelta64(1, 'h')
    return times

def npdt_to_str(times):
    '''Convert numpy datetime to string.'''
    # round to nearest minute and write in ISO-8601 format
    return pd.to_datetime(times.astype(str)).round('1min').strftime('%Y-%m-%dT%H:%M:%SZ').tolist()

def make_trajectory(x, y, z):
    '''Create a list of (x,y,z) points for a single trajectory.'''
    # check to make correct possible bug in starting times
    check_val = x[1]
    lx = len(x)
    if np.isnan(check_val):
        # find the last x None value
        i = 0
        while np.isnan(check_val) and i < lx - 1:
            i += 1
            if i == lx - 1:
                return []
            check_val = x[i + 1]
        # swap values if it makes sense
        if i < lx - 1:
            x[[0, i]] = x[[i, 0]]
            y[[0, i]] = y[[i, 0]]
            z[[0, i]] = z[[i, 0]]
    # Sometimes numpy uses non-standard float types that are rejected
    # by the geojson package. Use astype(float) to ensure they are
    # regular float-like.
    return list(zip(x.astype(float), y.astype(float), z.astype(float)))

def make_trajectories(ds):
    '''Create a MultiLineString containing the trajectories in a netcdf
    dataset.'''
    # fix times
    times = npdt_to_str(fix_times(ds['time'].values))
    
    # get all the lines
    trajs = []
    for i in range(ds.dims['traj']):
        x = ds.coords['xtraj'].isel(traj=i).values
        y = ds.coords['ytraj'].isel(traj=i).values
        z = ds['ptraj'].isel(traj=i).values
        trajs.append(make_trajectory(x, y, z))
    mlstring = geojson.MultiLineString(trajs)
    properties = {'times': times, 'aod': ds['aod_traj'].values.tolist()}
    return geojson.Feature(geometry=mlstring, properties=properties)


# 2) converting rasters (to postgresql)

# borrowed heavily from raster2psql.py, with a few changes to support python 3

import raster2psql
from raster2psql import *

def wkblify(fmt, data):
    """Writes raw binary data into HEX-encoded string using binascii module."""
    import struct

    # Binary to HEX
    fmt_little = '<' +fmt
    if isinstance(data, str):
        data = str.encode(data)
    hexstr = binascii.hexlify(struct.pack(fmt_little, data)).upper()

    # String'ify raw value for log
    valfmt = '\'' + fmt2printfmt(fmt[len(fmt) - 1]) + '\''
    val = valfmt % data
    logit('HEX (\'fmt=%s\', bytes=%d, val=%s):\t\t%s\n' \
          % (fmt, len(hexstr) / 2, str(val), hexstr))

    return hexstr.decode('utf-8')

# monkey patch wkblify
raster2psql.wkblify = wkblify

def wkblify_raster_level(options, ds, level, band_range, infile, i):
    assert ds is not None
    assert level >= 1
    assert len(band_range) == 2

    band_from = band_range[0]
    band_to = band_range[1]
    
    # Collect raster and block dimensions
    raster_size = ( ds.RasterXSize, ds.RasterYSize )
    if options.block_size is not None:
        block_size = parse_block_size(options)
        read_block_size = ( block_size[0] * level, block_size[1] * level)
        grid_size = calculate_grid_size(raster_size, read_block_size)
    else:
        block_size = raster_size # Whole raster as a single block
        read_block_size = block_size
        grid_size = (1, 1)

    logit("MSG: Processing raster=%s using read_block_size=%s block_size=%s of grid=%s in level=%d\n" % \
          (str(raster_size), str(read_block_size), str(block_size), str(grid_size), level))

    # Register base raster in RASTER_COLUMNS - SELECT AddRasterColumn();
    if level == 1:
        if i == 0 and options.create_table:
            gt = get_gdal_geotransform(ds)
            pixel_size = ( gt[1], gt[5] )
            pixel_types = collect_pixel_types(ds, band_from, band_to)
            nodata_values = collect_nodata_values(ds, band_from, band_to)
            extent = calculate_bounding_box(ds, gt)
            sql = make_sql_addrastercolumn(options, pixel_types, nodata_values,
                                           pixel_size, block_size, extent)
            options.output.write(sql)
            gen_table = options.table
            
    else:
        # Create overview table and register in RASTER_OVERVIEWS

        # CREATE TABLE o_<LEVEL>_<NAME> ( rid serial, options.column RASTER )
        schema_table_names = make_sql_schema_table_names(options.table)
        level_table_name = 'o_' + str(level) + '_' + schema_table_names[1] 
        level_table = schema_table_names[0] + '.' + level_table_name       
        if i == 0:
            sql = make_sql_create_table(options, level_table, True)
            options.output.write(sql)
            sql = make_sql_register_overview(options, level_table_name, level)
            options.output.write(sql)
            gen_table = level_table

    # Write (original) raster to hex binary output
    tile_count = 0
    hexwkb = ''

    for ycell in range(0, grid_size[1]):
        for xcell in range(0, grid_size[0]):

            xoff = xcell * read_block_size[0]
            yoff = ycell * read_block_size[1]

            logit("MSG: --------- CELL #%04d\tindex = %d x %d\tdim = (%d x %d)\t(%d x %d) \t---------\n" % \
                  (tile_count, xcell, ycell, xoff, yoff, xoff + read_block_size[0], yoff + read_block_size[1]))
            
            if options.block_size is not None:
                hexwkb = '' # Reset buffer as single INSERT per tile is generated
                hexwkb += wkblify_raster_header(options, ds, level, (xoff, yoff),
                                                block_size[0], block_size[1])
            else:
                hexwkb += wkblify_raster_header(options, ds, level, (xoff, yoff))

            for b in range(band_from, band_to):
                band = ds.GetRasterBand(b)
                assert band is not None, "Missing GDAL raster band %d" % b
                logit("MSG: Band %d\n" % b)

                hexwkb += wkblify_band_header(options, band)
                hexwkb += wkblify_band(options, band, level, xoff, yoff, read_block_size, block_size, infile, b).decode('utf-8')

            # INSERT INTO
            check_hex(hexwkb) # TODO: Remove to not to decrease performance
#             sql = make_sql_insert_raster(gen_table, options.column, hexwkb, options.filename, infile)
#             options.output.write(sql)
            
            tile_count = tile_count + 1

#     return (gen_table, tile_count)
    return hexwkb

def wkblify_raster(options, infile, i, previous_gt = None):
    """Writes given raster dataset using GDAL features into HEX-encoded of
    WKB for WKT Raster output."""
    
#     assert infile is not None, "Input file is none, expected file name"
#     assert options.version == g_rt_version, "Error: invalid WKT Raster protocol version"
#     assert options.endian == NDR, "Error: invalid endianness, use little-endian (NDR) only"
#     assert options.srid >= -1, "Error: do you really want to specify SRID = %d" % options.srid

    # Open source raster file
    ds = gdal.Open(infile, gdalc.GA_ReadOnly);
    if ds is None:
        sys.exit('Error: Cannot open input file: ' + str(infile))

    # By default, translate all raster bands

    # Calculate range for single-band request
    if options.band is not None and options.band > 0:
        band_range = ( options.band, options.band + 1 )
    else:
        band_range = ( 1, ds.RasterCount + 1 )

    # Compare this px size with previous one
    current_gt = get_gdal_geotransform(ds)
    if previous_gt is not None:
        if previous_gt[1] != current_gt[1] or previous_gt[5] != current_gt[5]:
            sys.exit('Error: Cannot load raster with different pixel size in the same raster table')

    # Generate requested overview level (base raster if level = 1)
    summary = wkblify_raster_level(options, ds, options.overview_level, band_range, infile, i)
#     SUMMARY.append( summary )
    
    # Cleanup
    ds = None

#     return current_gt
    return summary

def wkblify_raster2(options, infile, i, ds, previous_gt = None):
    """Writes given raster dataset using GDAL features into HEX-encoded of
    WKB for WKT Raster output."""
    
#     assert infile is not None, "Input file is none, expected file name"
#     assert options.version == g_rt_version, "Error: invalid WKT Raster protocol version"
#     assert options.endian == NDR, "Error: invalid endianness, use little-endian (NDR) only"
#     assert options.srid >= -1, "Error: do you really want to specify SRID = %d" % options.srid

    # Open source raster file
#     ds = gdal.Open(infile, gdalc.GA_ReadOnly);
    if ds is None:
        sys.exit('Error: Cannot open input file: ' + str(infile))

    # By default, translate all raster bands

    # Calculate range for single-band request
    if options.band is not None and options.band > 0:
        band_range = ( options.band, options.band + 1 )
    else:
        band_range = ( 1, ds.RasterCount + 1 )

    # Compare this px size with previous one
    current_gt = get_gdal_geotransform(ds)
    if previous_gt is not None:
        if previous_gt[1] != current_gt[1] or previous_gt[5] != current_gt[5]:
            sys.exit('Error: Cannot load raster with different pixel size in the same raster table')

    # Generate requested overview level (base raster if level = 1)
    summary = wkblify_raster_level(options, ds, options.overview_level, band_range, infile, i)
#     SUMMARY.append( summary )
    
    # Cleanup
#     ds = None

#     return current_gt
    return summary


# setting up raster binary
class RasterOptions(object):
    def __init__(self):
        if sys.byteorder == 'little':
            self.endian = 1
        else:
            self.endian = 0
        self.version = 0
        self.band = 0
        self.srid = 4326
        self.register = False
        self.overview_level = 1
        self.block_size = None
        self.create_table = False

def get_raster_binary(raster):
    i = 0
    options = RasterOptions()
    return wkblify_raster2(options, '', i, raster)

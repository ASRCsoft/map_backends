# serve json from IDEA database

# run from terminal with:
# export FLASK_APP=server.py
# python3 -m flask run --host=0.0.0.0 --with-threads --port=2112

# setup flask
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS, cross_origin
app = Flask(__name__)
outside_sites = ['http://pireds.asrc.cestm.albany.edu']
cors = CORS(app, resources={r"/*": {'origins': outside_sites}})

import io, psycopg2, datetime
from sqlalchemy import create_engine
import pandas as pd

pg = create_engine('postgresql:///lidar')
iso_fmt = '%Y-%m-%dT%H:%M:%S.000Z'

# set up some postGIS colormaps
# transparent-blue
transp_blue = '''100% 0 0 255 255
0% 0 0 255 0'''
# transparent-white
transp_white = '''100% 255 255 255 255
5 255 255 255 255
0 255 255 255 0'''
# blue-orange (w/ transparent missing data)
blue_orange = '''100% 251 141 4 255
1 251 141 4 255
0 55 104 251 255
nodata 0 0 0 0'''


# helper functions
def get_time_arg(req, arg):
    '''Get a datetime.datetime object from a url argument in an http request.'''
    time_str = req.args[arg]
    return datetime.datetime.strptime(time_str, iso_fmt)

def get_png(table, column, band, time, colormap, hr):
    '''Get a png from postgres'''
    band_query = ("select ST_AsPNG(ST_ColorMap(%s, %s, '%s'), '{1,2,3,4}'::int[]) from %s where time='%s' and high_resolution=%s" %
                  (column, band, colormap, table, time, hr))
    # get data from postgres
    with psycopg2.connect("dbname=lidar user=will") as conn:
        with conn.cursor() as cur:
            cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")
            cur.execute(band_query)
            res = cur.fetchone()
    return io.BytesIO(res[0].tobytes())

def get_wgs84_png(table, column, band, time, colormap, hr):
    '''Get a png from postgres'''
    band_query = ("select ST_AsPNG(ST_ColorMap(ST_Transform(ST_Band(%s, %s), 3857), 1, '%s'), '{1,2,3,4}'::int[]) from %s where time='%s' and high_resolution=%s" %
                  (column, band, colormap, table, time, hr))
    # get data from postgres
    with psycopg2.connect("dbname=lidar user=will") as conn:
        with conn.cursor() as cur:
            cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")
            cur.execute(band_query)
            res = cur.fetchone()
    return io.BytesIO(res[0].tobytes())


# get most recent simulation
@app.route('/most_recent', methods=['GET'])
def most_recent():
    print(request.args)
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    site_query = ("select lower(time_range) as start_time, upper(time_range) as end_time from idea.simulations where high_resolution=%s order by lower(time_range) desc limit 1" %
                  high_resolution)
    df = pd.read_sql(site_query, pg)
    return df.to_json(orient='records', date_format='iso', date_unit='s')

# get trajectories
@app.route('/trajectories', methods=['GET'])
def trajectories():
    print(request.args)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', type=str)
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    site_query = ("select lower(time_range) as start_time, upper(time_range) as end_time, trajectories from idea.simulations where high_resolution=%s and time_range && '(%s, %s)' order by lower(time_range)" %
                  (high_resolution, start_time, end_time))
    df = pd.read_sql(site_query, pg)
    return df.to_json(orient='records', date_format='iso', date_unit='s')


# get NWP raster

# map variable names to band numbers
p_heights = [500, 700, 850]
ds_names = ([ 'uwind%s' % h for h in p_heights ] +
            [ 'vwind%s' % h for h in p_heights ] +
            ['apcp'])
nwp_dict = { s: i + 1 for i, s in enumerate(ds_names) }
@app.route('/nwp', methods=['GET'])
def nwp():
    print(request.args)
    time = request.args.get('time', type=str)
    band_names = request.args.getlist('band', type=str)
    bands = [ nwp_dict[s] for s in band_names ]
    bands_str = ','.join(map(str, bands))
    band_query = "select ST_AsTIFF(ST_Band(nwp, '{%s}'::int[])) from idea.nwp where time='%s' and not high_resolution" % (bands_str, time)
    # get data from postgres
    with psycopg2.connect("dbname=lidar user=will") as conn:
        with conn.cursor() as cur:
            cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")
            cur.execute(band_query)
            res = cur.fetchone()
    tiff_bin = res[0].tobytes()
    return send_file(io.BytesIO(tiff_bin),
                     attachment_filename='nwp.tif',
                     mimetype='application/x-geotiff')

@app.route('/wind', methods=['GET'])
def wind():
    print(request.args)
    time = request.args['time']
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    if high_resolution:
        # only PBL winds available
        bands = [1, 2]
    else:
        height = request.args.get('height', type=int)
        height_n = p_heights.index(height) + 1
        bands = [height_n, height_n + 3]

    # calculate direction in degrees from the 2 horizontal wind speed
    # components (the leaflet geotiff add-on requires positive
    # degrees, measured clockwise from due south)
    q = ("select ST_AsTIFF(ST_MapAlgebra(nwp, %s, nwp, %s, '(atan2d(-[rast1],-[rast2])+360)::numeric %% 360')) as direction from idea.nwp where time='%s' and high_resolution=%s" %
         (bands[0], bands[1], time, high_resolution))
    # get data from postgres
    with psycopg2.connect("dbname=lidar user=will") as conn:
        with conn.cursor() as cur:
            cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")
            cur.execute(q)
            res = cur.fetchone()
    tiff_bin = res[0].tobytes()
    return send_file(io.BytesIO(tiff_bin),
                     attachment_filename='wind.tif',
                     mimetype='application/x-geotiff')

# kind of similar but get a png instead
@app.route('/apcp', methods=['GET'])
def apcp():
    '''Get a png of precipitation in a nice transparent-blue color
    scale.

    '''
    print(request.args)
    time = get_time_arg(request, 'time')
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    if high_resolution:
        # only PBL winds available
        apcp_band = 3
    else:
        apcp_band = 7
    return send_file(get_wgs84_png('idea.nwp', 'nwp', apcp_band, time, transp_blue, high_resolution),
                     attachment_filename='apcp.png',
                     mimetype='image/png')

@app.route('/cod', methods=['GET'])
def cod():
    '''Get a png of cloud optical depth in a nice transparent-white color
    scale.

    '''
    print(request.args)
    time = get_time_arg(request, 'time')
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    return send_file(get_png('viirs.swaths', 'swath', 2, time, transp_white, high_resolution),
                     attachment_filename='apcp.png',
                     mimetype='image/png')

@app.route('/aod', methods=['GET'])
def aod():
    '''Get a png of aerosol optical depth in a nice blue-orange color
    scale.

    '''
    print(request.args)
    time = get_time_arg(request, 'time')
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    return send_file(get_png('viirs.swaths', 'swath', 1, time, blue_orange, high_resolution),
                     attachment_filename='apcp.png',
                     mimetype='image/png')

@app.route('/swaths', methods=['GET'])
def swaths():
    '''Get the available swath times and domains in a given time interval.
    '''
    print(request.args)
    start_time = get_time_arg(request, 'start')
    end_time = get_time_arg(request, 'end')
    if 'resolution' in request.args.keys():
        high_resolution = request.args['resolution'] == 'high'
    else:
        high_resolution = False
    site_query = "select time, ST_AsGeoJSON(ST_Envelope(swath)) as bounds from viirs.swaths where time between '%s' and '%s' and high_resolution=%s order by time asc" % (start_time, end_time, high_resolution)
    df = pd.read_sql(site_query, pg)
    return df.to_json(orient='records', date_format='iso', date_unit='s')

# band_query = "select ST_AsTIFF(ST_Band(nwp, '{%s}'::int[])) from idea.nwp where time='%s'" % (1, '2018-07-01')
# with psycopg2.connect("dbname=lidar user=will") as conn:
#     with conn.cursor() as cur:
#         cur.execute(band_query)
#         res = cur.fetchone()


# print(request.args)
# time = request.args.get('time', type=str)
# band_query = "select ST_AsPNG(ST_ColorMap(nwp, 7, ), '{7,7,7,7}'::int[]) from idea.nwp where time='%s'" % time
# band_query = "select ST_AsPNG(ST_ColorMap(nwp, 7, '%s'), '{1,2,3,4}'::int[]) from idea.nwp where time='%s'" % (apcp_colors, time)
# # get data from postgres
# with psycopg2.connect("dbname=lidar user=will") as conn:
#     with conn.cursor() as cur:
#         cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")
#         cur.execute(band_query)
#         res = cur.fetchone()
# png_bin = res[0].tobytes()

# useful utilities for dealing with hysplit

import glob, os, datetime, warnings
import matplotlib as mpl
# mpl.use('Agg')
# import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator
import copy as cp
import numpy as np
import pandas as pd
import xarray as xr
import json
import geojson
from subprocess import call
# from sqlalchemy import create_engine
# pg = create_engine('postgresql:///hysplit_xcite')

def get_contours(p):
    """"Get contours from a contour plot"""
    contours = []
    for collection in p.collections:
        contours.append(collection.get_paths())
    return contours

def get_polygons(contour):
    polygons = []
    for path in contour:
        polygons.append(list(map(lambda x: x.tolist(), path.to_polygons())))
    return polygons

def get_multipolygon(contour):
    """"Turn a set of contour paths into a multipolygon"""
    return geojson.MultiPolygon(get_polygons(contour))

def make_feature(contour, level, color, deposition=False):
    """"Create a geojson 'feature'"""
    gjson = geojson.Feature(geometry=get_multipolygon(contour))
    if deposition:
        units = 'mass/m<sup>2</sup>'
    else:
        units = 'mass/m<sup>3</sup>'
    gjson.properties['level_name'] = '10<sup>' + str(int(level)) + '</sup> ' + units
    gjson.properties['level'] = int(level)
    return gjson

def contour2geojson(contour, level, color, deposition=False):
    return make_feature(contour, level, color, deposition)

def make_json(p, height, cmap='jet'):
    contours = get_contours(p)
    levels = p.levels
    ncontours = len(contours)
    # get the colors for each contour
    cmap = mpl.cm.get_cmap(cmap, ncontours)
    colors = []
    for i in range(cmap.N):
        rgb = cmap(i)[:3] # will return rgba, we take only first 3 so we get rgb
        colors.append(mpl.colors.rgb2hex(rgb))
    # get the contour MultiPolygons
    contours_list = []
    for i in range(ncontours):
        contour = contours[i]
        color = colors[i]
        level = levels[i]
        if len(contour) == 0:
            continue
        if height == 0:
            contours_list.append(make_feature(contour, level, color, deposition=True))
        else:
            contours_list.append(make_feature(contour, level, color))
    return geojson.FeatureCollection(contours_list)

def contours2geojson(p, height, cmap='jet'):
    return make_json(p, height, cmap='jet')

def matrix2geojsoncontours():
    pass

def add_contours_to_db(pg, filename, sim_id, height, time):
    with open(filename, 'r') as topofile:
        topo_str = topofile.read()
    if sim_id is None:
        sim_str = 'null'
    else:
        sim_str = str(sim_id)
    query = ('insert into contours values (' + sim_str +
             ',' + str(height) + ',' + str(time) +
             ",'" + topo_str +
             "') on conflict (simulation_id,height,time) do update set topojson=excluded.topojson where contours.simulation_id=excluded.simulation_id and contours.height=excluded.height and contours.time=excluded.time")
    with pg.connect() as con:
        con.execute(query)

def write_contour_files(pg, fwd, hysplit, site_folder, loglevels,
                        quantize=10000, sim_id=None):
    # prepare the matplotlib objects needed for contours
    fig = Figure()
    ax = fig.add_subplot(111)
    
    # make the topojson files
    x = hysplit.coords['longitude'].values
    y = hysplit.coords['latitude'].values
    hylevels = hysplit.coords['levels'].values
    hytimes = hysplit.coords['time'].values
    for i, h in enumerate(hysplit.coords['levels']):
        for j in range(hysplit.dims['time']):
            time_ind = j
            if not fwd:
                time_ind = hysplit.dims['time'] - 1 - j
            z = hysplit['log10PM'].isel(time=time_ind, levels=i)
            p2 = ax.contourf(x, y, z, levels=np.array(loglevels))
            gjson = make_json(p2, height=h)
            # clear away old contours
            ax.collections = []
            fname = 'height' + str(i) + '_time' + str(j)
            geofile = site_folder + fname + '.geojson'
            topofile = site_folder + fname + '.json'
            with open(geofile, 'w') as outfile:
                geojson.dump(gjson, outfile)
            call(['geo2topo', geofile, '-o', topofile, '-q', str(quantize)])
            # now add the topojson to postgres
            add_contours_to_db(pg, topofile, sim_id, i, j)
            # don't need to do this anymore because I'm deleting the entire folder afterwards:
            # # remove unnecessary files
            # call(['rm', geofile])
            # # can call this later when everything is switched to postgres:
            # # call(['rm', geofile, topofile])

def npdt_to_str(times):
    # convert numpy datetime to string
    # round to nearest minute and write in ISO-8601 format
    return pd.to_datetime(times.astype(str)).round('1min').strftime('%Y-%m-%dT%H:%M:%SZ').tolist()

def make_trajectory_feature(tr_df, times):
    linestring = geojson.LineString(tr_df[[10, 9]].values.tolist())
    feature = geojson.Feature(geometry=linestring)
    feature.properties['times'] = npdt_to_str(times)
    feature.properties['heights'] = tr_df[11].values.tolist()
    return feature

def make_trajectories(traj_file, times):
    col_widths = [6, 6, 6, 6, 6, 6, 6, 6, 8, 9, 9, 9, 9]
    tr_df = pd.read_fwf(traj_file, widths = col_widths, header=None, skiprows=31)

    # add ensemble deltas:    
    # DATA DX / 3*0.0, 3*1.0, 3*-1.0, 3*0.0, 3*1.0, 3*-1.0, 3*0.0, 3*1.0, 3*-1.0 /
    # DATA DY / 0.,1.,-1.  ,0.,1.,-1.  ,0.,1.,-1.  ,0.,1.,-1.  ,0.,1.,-1.   &
    # ,0.,1.,-1.  ,0.,1.,-1.  ,0.,1.,-1.  ,0.,1.,-1. /
    # DATA DZ / 9*0., 9*1.0, 9*-1.0 /

    # hahahaha this makes no sense at all man
    # dx = np.array(([0] * 3 + [1] * 3 + [-1] * 3) * 3)
    # dy = np.array([0, 1, -1] * 9)
    # dz = np.array([0] * 9 + [1] * 9 + [-1] * 9)
    # gridkm = 15
    
    # # need to convert km to lat and lon differences
    # for i in range(27):
    #     lat1 = tr_df[9][i]
    #     lon1 = tr_df[10][i]
    #     origin = geopy.Point(lat1, lon1)
    #     # move in the x direction
    #     pointx = VincentyDistance(kilometers=dx[i] * gridkm).destination(origin, 0)
    #     origin = geopy.Point(pointx.latitude, pointx.longitude)
    #     # move in the y direction
    #     pointy = VincentyDistance(kilometers=dy[i] * gridkm).destination(origin, 90)
    #     # origin = geopy.Point(pointy.latitude, pointy.longitude)
    #     # fix the point in the dataframe
    #     tr_df.iloc[i, 9] = pointy.latitude
    #     tr_df.iloc[i, 10] = pointy.longitude
            
    # get all the lines
    features = []
    for n in tr_df[0].unique():
        tr_df2 = tr_df.loc[tr_df[0] == n]
        features.append(make_trajectory_feature(tr_df2, times))
    return geojson.FeatureCollection(features)

class HysplitControl:
    def __init__(self, file):
        self.file_path = file
        with open(self.file_path, 'r') as f:
            self.file_lines = f.read().split('\n')
        self.time_format = '%y %m %d %H'
        # self.time_format2 = '%y %m %d %H %M'
        self.start_time = datetime.datetime.strptime(self.file_lines[0], self.time_format)
        self.n_sites = int(self.file_lines[1])
        line2 = self.file_lines[2].split(' ')
        self.latitude = float(line2[0])
        self.longitude = float(line2[1])
        self.meters_agl = float(line2[2])
        line3 = int(self.file_lines[3])
        self.forward = line3 > 0
        self.n_periods = abs(line3)

    def get_times(self, offset):
        np_start_time = np.datetime64(self.start_time)
        start_range = np.timedelta64(offset, 'h')
        end_range = np.timedelta64(self.n_periods + 1, 'h')
        time_range = np.arange(start_range, end_range, dtype='timedelta64[h]')
        if self.forward:
            return np_start_time + time_range
        else:
            return np_start_time - time_range
        
class TrajectoryControl(HysplitControl):
    def __init__(self, file):
        super(TrajectoryControl, self).__init__(file)
        self.output_file = self.file_lines[10]

    @property
    def times(self):
        return self.get_times(0)
    
class ConcentrationControl(HysplitControl):
    def __init__(self, file):
        super(ConcentrationControl, self).__init__(file)
        self.release_duration = float(self.file_lines[12])
        # self.release_time = datetime.datetime.strptime(self.file_lines[13], self.time_format2)
        self.output_file = self.file_lines[19]
        self.height_bounds = list(map(float, self.file_lines[21].split(' ')))
        self.deposition_settings = list(map(float, self.file_lines[26].split(' ')))
        # if any of these is zero deposition is not modeled
        self.deposition = not any(map(lambda x: not x, self.deposition_settings))

    @property
    def times(self):
        return self.get_times(1)
    
def control_json(control):
    # convert hysplit control object to json
    meta = {}
    meta['release_time'] = str(control.start_time)
    meta['latitude'] = control.latitude
    meta['longitude'] = control.longitude
    meta['release_height'] = control.meters_agl
    meta['release_duration'] = control.release_duration
    return meta

def get_ens_trajectories(fwd, data_dir, control):
    # get the trajectory coordinate times from the control file
    if fwd:
        fwd_str = 'fwd'
    else:
        fwd_str = 'bwd'
    if data_dir == './':
        trajectory_file = data_dir + control.output_file
    else:
        trajectory_file = data_dir + control.output_file + '_' + fwd_str + '_ens'
    return make_trajectories(trajectory_file, control.times)

def get_trajectory(fwd, data_dir, control):
    # get the trajectory coordinate times from the control file
    if fwd:
        fwd_str = 'fwd'
    else:
        fwd_str = 'bwd'
    if data_dir == './':
        trajectory_file = data_dir + control.output_file
    else:
        trajectory_file = data_dir + control.output_file + '_' + fwd_str
    col_widths = [6, 6, 6, 6, 6, 6, 6, 6, 8, 9, 9, 9, 9]
    tr_df = pd.read_fwf(trajectory_file, widths = col_widths, header=None, skiprows=5)
    return make_trajectory_feature(tr_df, control.times)

def make_metajson(data_dir, fwd, hysplit, conc_control, traj_ens_control,
                  traj1_control, site_folder, levels):
        # organize the metadata json
    metadata = control_json(conc_control)
    # the contour times
    meta_times = npdt_to_str(conc_control.times)
    if fwd:
        metadata['times'] = meta_times
    else:
        metadata['times'] = list(reversed(meta_times))
    metadata['heights'] = hysplit.coords['levels'].values.tolist()
    metadata['trajectories'] = get_ens_trajectories(fwd, data_dir, traj_ens_control)
    metadata['trajectory'] = get_trajectory(fwd, data_dir, traj1_control)
    # add lat/lon (needed for custom simulations)
    metadata['latitude'] = conc_control.latitude
    metadata['longitude'] = conc_control.longitude
    metadata['levels'] = levels
    return metadata

def write_meta_file(data_dir, fwd, hysplit, conc_control,
                    traj_ens_control, traj1_control, site_folder):
    metadata = make_metajson(data_dir, fwd, hysplit,
                             conc_control, traj_ens_control,
                             traj1_control, site_folder)
    meta_file = site_folder + 'meta.json'
    with open(meta_file, 'w') as outfile:
        json.dump(metadata, outfile)

def add_metadata_to_db(pg, sim_id, metadata):
    # time_str = '{' + str(list(map(str, metadata['times'])))[1:-1] + '}'
    # height_str = '{' + str(metadata['heights'])[1:-1] + '}'
    query = ("update simulations set metadata='" +
             json.dumps(metadata) + "' where id=" + str(sim_id))
    with pg.connect() as con:
        con.execute(query)

def write_json_files(pg, site, fwd, site_folder0, quantize, data_dir,
                     controls=None, sim_id=None):
    if fwd:
        fwd_str = 'fwd'
        tr1_id = '001'
        tr_ens_id = '005'
        conc_id = '010'
    else:
        fwd_str = 'bwd'
        tr1_id = '002'
        tr_ens_id = '006'
        conc_id = '011'
    fwd_folder = fwd_str + '/'
    site_folder = site_folder0 + fwd_folder
    if not os.path.exists(site_folder):
        os.makedirs(site_folder)
    else:
        if controls is None:
            # remove old data if it this is not a custom simulation
            for f in glob.glob(site_folder + '*'):
                os.remove(f)
    if controls is None:
        tr1_control = TrajectoryControl(data_dir + 'CONTROL.' + tr1_id + '_' + site)
        tr_ens_control = TrajectoryControl(data_dir + 'CONTROL.' + tr_ens_id + '_' + site)
        conc_control = ConcentrationControl(data_dir + 'CONTROL.' + conc_id + '_' + site)
        nc_file = data_dir + conc_control.output_file + '_' + fwd_str + '.ncf'
    else:
        tr1_control = TrajectoryControl(controls['single_trajectory'])
        tr_ens_control = TrajectoryControl(controls['ens_trajectory'])
        conc_control = ConcentrationControl(controls['concentration'])
        nc_file = 'cdump.nc'
    # get hysplit data from netcdf
    hysplit = xr.open_dataset(nc_file)
    # remove useless data
    if not conc_control.deposition:
        hysplit = hysplit.drop(0, 'levels')
    # # convert to nanograms
    # if 'PM' in list(hysplit.keys()):
    #     hysplit['PM'] = hysplit['PM'] * 10**9
    # else:
    #     hysplit['PM'] = hysplit['TEST'] * 10**9
    if 'PM' not in list(hysplit.keys()):
        hysplit['PM'] = hysplit['TEST']
    # get log values
    with warnings.catch_warnings():
        # don't want to see log of zero warnings
        warnings.simplefilter("ignore")
        hysplit['log10PM'] = np.log10(hysplit['PM'])

    # what I should probably do: get the levels first, then replace
    # -inf with (min(levels) - 1) -- well I guess get the level
    # interval size first and do (min(levels) - dlevels)
    min_level = hysplit['log10PM'].where(~np.isinf(hysplit['log10PM'])).min().values
    max_level = hysplit['log10PM'].where(~np.isinf(hysplit['log10PM'])).max().values
    # now get nice tick intervals
    zloc = MaxNLocator(nbins=8).tick_values(min_level, max_level)
    dz = zloc[1] - zloc[0]
    
    hysplit['log10PM'].values = np.where(np.isinf(hysplit['log10PM']), zloc[0] - dz, hysplit['log10PM'])
    # contour levels
    loglevels = list(range(-17, -10))
    loglevels.append(-5)
    # loglevels = zloc
    # loglevels = np.append(loglevels, zloc[-1] + 999) # top bin should get everything
    
    # make the topojson files
    # print('Starting contours...')
    write_contour_files(pg, fwd, hysplit, site_folder, loglevels,
                        quantize, sim_id)
    
    # get trajectories and write meta.json files
    meta_times = npdt_to_str(hysplit.coords['time'])
    if fwd:
        n_str2 = '005'
    else:
        n_str2 = '006'
    # print('Starting metadata...')
    # write_meta_file(data_dir, fwd, hysplit, conc_control, tr_ens_control, tr1_control, site_folder)
    metadata = make_metajson(data_dir, fwd, hysplit,
                             conc_control, tr_ens_control,
                             tr1_control, site_folder, loglevels)
    # meta_file = site_folder + 'meta.json'
    # with open(meta_file, 'w') as outfile:
    #     json.dump(metadata, outfile)
    # add it to postgres
    add_metadata_to_db(pg, sim_id, metadata)
    
    # close netcdf file
    hysplit.close()

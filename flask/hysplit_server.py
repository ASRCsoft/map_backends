# a simple flask utility for running HYSPLIT given latitude and
# longitude values passed as url arguments

# run from terminal with:
# export FLASK_APP=hysplit.py
# nohup /usr/bin/python3 -m flask run --host=0.0.0.0 --with-threads &
# setup flask
from flask import Flask, request, make_response
from flask_cors import CORS, cross_origin
app = Flask(__name__)
outside_sites = ['http://pireds.asrc.cestm.albany.edu',
                 'http://pireds.asrc.cestm.albany.edu:9999',
                 'http://appsvr.asrc.cestm.albany.edu',
                 'nysmesonet.org', 'www.nysmesonet.org',
                 'xwww.nysmesonet.org']
cors = CORS(app, resources={r"/*": {'origins': outside_sites}})

import json, os, datetime, time, shutil
from subprocess import call
from sqlalchemy import create_engine
import pandas as pd
import hysplit_common
# This 'engine' is a connection manager, *NOT* the connection
# itself. No need to close it.
pg = create_engine('postgresql:///hysplit_xcite')

# number of processors for parallel processing
nprocessors = 4
# default control files
single_traj_default = 'CONTROL_single_traj'
ens_traj_default = 'CONTROL_ens_traj'
conc_default = 'CONTROL_conc'
# simulation control files
single_traj_file = 'CONTROL_single_traj'
ens_traj_file = 'CONTROL_ens_traj'
conc_file = 'CONTROL_conc'
# website data directory
data_dir = '/home/xcite/hysplit/conversion_data/'
# directory where I have all the HYSPLIT control template files
hysplit_dir = '/home/xcite/hysplit/'
# default release height, in meters
default_height = 250
# topojson quantization setting
quantize = 10000

# going to:

# 1) get the request values
# 2) assign an id
# 3) create a new folder and cd to it
# 4) create appropriate config files
# 5) run hysplit
# -- rename appropriate config 'CONTROL', run model, repeat
# 6) send back the id

# class HysplitFailure(Exception):
#     """Raised when HYSPLIT stops with value 900"""
#     def __init__(self):
#         # Call the base class constructor with the parameters it needs
#         message = 'HYSPLIT simulation failed'
#         super(HysplitFailure, self).__init__(message)
    

def assign_id():
    return uuid.uuid1()

def assign_id2(time_id, fwd):
    query = ("insert into simulations (site_id, time_id, forward) values (null," +
             str(time_id) + ",'" + str(fwd) + "') returning id")
    with pg.connect() as con:
        rs = con.execute(query)
    return list(rs)[0][0]

def get_time_from_id(time_id):
    query = "select time from available_times where id=" + str(time_id)
    with pg.connect() as con:
        rs = con.execute(query)
    return list(rs)[0][0]

def get_met_file(time):
    # put together the met data file name
    # 2) get the file times
    time1 = time - datetime.timedelta(days=1)
    if time.hour == 0:
        time1 = time1.replace(hour=6)
    else:
        time1 = time1.replace(hour=18)
    time_format = '%Y%m%d.%Hz'
    time1_str = time1.strftime(time_format)
    time2_str = time.strftime(time_format)
    return 'hysplit.hrrr.' + time1_str + '-' + time2_str + '.sml'

def update_control(control, options):
    # get control options
    lat = options.get('lat', type=float)
    lon = options.get('lon', type=float)
    fwd_str = options.get('fwd', type=str)
    if fwd_str == 'fwd':
        fwd = True
    else:
        fwd = False
    time_id = options.get('time_id', type=int)
    height = options.get('height', type=float)
    records = options.get('records', type=int)

    start_time = get_time_from_id(time_id)
    print('Start time:')
    print(start_time)
    control[0] = start_time.strftime('%y %m %d %H') + '\n'
    control[2] = ' '.join(map(str, [lat, lon, height])) + '\n'
    control[3] = str(records) + '\n'
    if not fwd:
        control[3] = '-' + control[3]
    control[8] = get_met_file(start_time) + '\n'
    with open('CONTROL', 'w') as f:
        f.writelines(control)
    return control

def run_hysplit_variant(default, control_file, options, command):
    control = update_control(default, options)
    # this control file isn't used by hysplit, it's just there to save
    # the control settings used for the run, in case someone wants to
    # look at it later
    with open(control_file, 'w') as f:
        f.writelines(control)
    # this is for parallel processing:
    response = call(['mpirun', '-np', str(nprocessors), command])
    # response = call(command)
    if int(response) == 132:
        # This value is the result of the 'STOP 900' line from
        # hysplit, indicating something went wrong. The linux exit
        # codes only go up to 255 so 900 turns into 132 on the linux
        # shell.
        raise Exception('HYSPLIT simulation failed')

def run_single_traj(options, default):
    # copy setup.cfg file first
    shutil.copy('/home/xcite/hysplit/SETUP.trj.CFG', './SETUP.CFG')
    run_hysplit_variant(default, single_traj_file, options, 'hytm_std')

def run_ens_traj(options, default):
    # copy setup.cfg file first
    shutil.copy('/home/xcite/hysplit/SETUP.trj.CFG', './SETUP.CFG')
    # command doesn't exist!
    # run_hysplit_variant(default, ens_traj_file, options, 'hytm_ens')
    control = update_control(default, options)
    with open(ens_traj_file, 'w') as f:
        f.writelines(control)
    call('hyts_ens')

def run_conc(options, default):
    # copy setup.cfg file first
    shutil.copy('/home/xcite/hysplit/SETUP.dis.CFG', './SETUP.CFG')
    run_hysplit_variant(default, conc_file, options, 'hycm_std')
    # convert cdump binary file to netcdf
    call(['conc2cdf', '-icdump', '-ocdump.nc'])

def run_hysplit(options, sim_id):
    # make sure we start in the right folder
    os.chdir(hysplit_dir)
    
    # get the default control files
    with open(single_traj_default, 'r') as f:
        single_traj_control = f.readlines()
    with open(ens_traj_default, 'r') as f:
        ens_traj_control = f.readlines()
    with open(conc_default, 'r') as f:
        conc_control = f.readlines()

    # move to the data directory
    print(str(sim_id))
    site_dir = data_dir + str(sim_id) + '/'
    if not os.path.exists(site_dir):
        os.mkdir(site_dir)
    fwd = options.get('fwd', type=str) == 'fwd'
    fwd_dir = site_dir + ('fwd' if fwd else 'bwd') + '/'
    if not os.path.exists(fwd_dir):
        os.mkdir(fwd_dir)
    # copy ASCDATA.CFG
    shutil.copyfile('ASCDATA.CFG', fwd_dir + 'ASCDATA.CFG')
    os.chdir(fwd_dir)

    # run the simulations
    run_single_traj(options, single_traj_control)
    run_ens_traj(options, ens_traj_control)
    run_conc(options, conc_control)

    # single trajectory output gets written to the wrong file with
    # parallel processing, change it back
    single_traj_output = single_traj_control[10].rstrip()
    single_traj_output_wrong = single_traj_output + '.002'
    shutil.copy(single_traj_output_wrong, single_traj_output)
    # call(['cp', single_traj_output_wrong, single_traj_output])

    # convert to topojson etc
    controls = {'single_trajectory': single_traj_file,
                'ens_trajectory': ens_traj_file,
                'concentration': conc_file}
    hysplit_common.write_json_files(pg, str(sim_id) + '/', fwd,
                                    site_dir, quantize, './', controls,
                                    sim_id)

    # remove the unneeded directory
    shutil.rmtree(site_dir)

    # return the new id
    return sim_id

def get_metadata(site_id, time_id, fwd, sim_id=None):
    if sim_id is not None:
        # get the metadata json as a string
        query = ("select jsonb_build_object('id', id, 'metadata', metadata)::text from simulations where id=" +
                 str(sim_id))
    else:
        # get the metadata json as a string
        query = ("select jsonb_build_object('id', id, 'metadata', metadata)::text from simulations where site_id=" +
                 str(site_id) + " and time_id=" + str(time_id) +
                 " and forward=" + str(fwd))
    with pg.connect() as con:
        rs = con.execute(query)
    return list(rs)[0][0]

def get_contours(sim_id, height, time):
    # get the metadata json as a string
    query = ("select topojson::text from contours where simulation_id=" +
             str(sim_id) + " and height=" + str(height) +
             " and time=" + str(time))
    with pg.connect() as con:
        rs = con.execute(query)
    return list(rs)[0][0]


@app.route('/', methods=['POST', 'GET'])
def hysplit():
    if request.method == 'POST':
        # get the parameters
        options = request.args.copy()
        fwds = request.args.getlist('fwd', type=str)
        results = {}

        # get an id for the simulation
        sim_id = assign_id()
        print(sim_id)
        results['id'] = str(sim_id)
        # run hysplit and get simulation ID
        start_time = time.time()
        if 'true' in fwds:
            options.setlist('fwd', ['fwd'])
            try:
                run_hysplit(options, sim_id)
            except Exception as e:
                results['error'] = str(e)
        if 'false' in fwds:
            options.setlist('fwd', ['bwd'])
            try:
                run_hysplit(options, sim_id)
            except Exception as e:
                results['error'] = str(e)
        end_time = time.time()
        results['seconds'] = round(end_time - start_time)
        return json.dumps(results)
    
    else:
        # if we mucked something up the request
        return 'Must send a POST request to run HYSPLIT, instead sent a ' + request.method + ' request.'

@app.route('/hysplit2', methods=['POST', 'GET'])
def hysplit2():
    if request.method in ['POST', 'GET']:
        # get the parameters
        options = request.args.copy()
        time_id = request.args.get('time_id', type=int)
        fwds = request.args.getlist('fwd', type=str)
        results = {}
        # run hysplit and get simulation ID
        start_time = time.time()
        if 'true' in fwds:
            # get an id for the simulation
            results['fwd'] = sim_id = assign_id2(time_id, True)
            print('sim_id:')
            print(sim_id)
            options.setlist('fwd', ['fwd'])
            # try:
            run_hysplit(options, sim_id)
            # except Exception as e:
            #     results['error'] = str(e)
        else:
            results['fwd'] = None
        if 'false' in fwds:
            # get an id for the simulation
            results['bwd'] = sim_id = assign_id2(time_id, False)
            options.setlist('fwd', ['bwd'])
            try:
                run_hysplit(options, sim_id)
            except Exception as e:
                results['error'] = str(e)
        else:
            results['bwd'] = None
        end_time = time.time()
        results['seconds'] = round(end_time - start_time)
        return json.dumps(results)
    
    else:
        # if we mucked something up the request
        return 'Must send a POST request to run HYSPLIT, instead sent a ' + request.method + ' request.'


# get the available sites!
@app.route('/sites', methods=['GET'])
def sites():
    site_query = 'select id, stid, name, latitude, longitude from sites order by name'
    df = pd.read_sql(site_query, pg)
    return df.to_json(orient='records')

# get the available times!
@app.route('/times', methods=['GET'])
def times():
    site_query = "select id, time from available_times where active='true' order by time desc"
    df = pd.read_sql(site_query, pg, index_col='id')
    return df.to_json(orient='split', date_format='iso', date_unit='s')

# get metadata
@app.route('/metadata', methods=['GET'])
def metadata():
    print(request.args)
    if 'sim_id' in request.args:
        sim_id = request.args.get('sim_id', type=int)
        return get_metadata(None, None, None, sim_id)
    site_id = request.args.get('site_id', type=int)
    time_id = request.args.get('time_id', type=int)
    fwd = request.args.get('fwd', type=str) == 'true'
    return get_metadata(site_id, time_id, fwd)

# get contours
@app.route('/contours', methods=['GET'])
def contours():
    print(request.args)
    sim_id = request.args.get('sim_id', type=int)
    height = request.args.get('height', type=int)
    time = request.args.get('time', type=int)
    return get_contours(sim_id, height, time)

    
# ooooooh this is useful
def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    
@app.route('/shutdown', methods=['Get'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

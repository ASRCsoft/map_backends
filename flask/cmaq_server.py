# get CMAQ profile plots

# run from terminal with:
# export FLASK_APP=server.py
# python3 -m flask run --host=0.0.0.0 --with-threads -p 5679
from flask import Flask, request, make_response
from flask_cors import CORS, cross_origin
app = Flask(__name__)
outside_sites = ['http://pireds.asrc.cestm.albany.edu',
                 'http://pireds.asrc.cestm.albany.edu:9999',
                 'http://appsvr.asrc.cestm.albany.edu',
                 'nysmesonet.org', 'www.nysmesonet.org',
                 'xwww.nysmesonet.org']
cors = CORS(app, resources={r"/*": {'origins': outside_sites}})

import datetime, time
import pandas as pd
import xarray as xr
# from sqlalchemy import create_engine
# pg = create_engine('postgresql:///hysplit_xcite')
cmaq_ds = xr.open_dataset('.nc')


@app.route('/cmaq', methods=['GET'])
def cmaq():
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
    end_time = time.time()
    results['seconds'] = round(end_time - start_time)
    return json.dumps(results)
    

    
# useful
def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    
@app.route('/shutdown', methods=['Get'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'
